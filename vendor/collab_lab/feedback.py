"""Tool effects and compact facts; never select the agent's next action."""
import copy
from .runtime import explicit_versions


DEVICE_EFFECT = {
    'changes': ['所选设备的管理启用开关enabled和信道channel'],
    'does_not': ['不迁移业务路由', '不修改业务源速率', '不能恢复物理up或增加容量'],
    'follow_up_if_needed': '业务仍沿原路由传输；如需迁移，可查询list_routes，使用simulate_services推演、apply_services配置；是否需要由端侧判断。',
}

EFFECTS = {
    'read_sensor_adapter': (['取得新的现场观测回执'], ['不改变配置，不提供处置方案']),
    'inspect_links': (['主动采样并返回链路事实'], ['不修复链路，不改变配置']),
    'inspect_services': (['主动采样并返回业务实测'], ['不迁移业务，不改变配置']),
    'list_routes': (['计算所引用观测的全部路径选项'], ['不选择路径，不启用设备，不迁移业务']),
    'simulate_services': (['在副本预测业务路由或源速率调整效果'], ['不改变现场，预测达标不等于实际达标']),
    'apply_services': (['实际修改指定业务的路由或源速率'], ['不启用设备，不修改设备协议或物理容量；动作成功不保证业务达标']),
    'read_catalog': (['读取基础和已发布工具及职责'], ['不制作工具，不执行配置']),
    'retrieve_knowledge': (['检索已有知识和策略'], ['不读取当前现场，不执行策略，不保证经验适用']),
    'check_resource_feasibility': (['检查观测中物理路径和单路径容量的必要边界'], ['不执行修复；必要条件通过不证明共享资源分配可行']),
    'get_request_status': (['查询原请求执行状态'], ['不重放操作，unknown不等于成功']),
    'get_receipt': (['读取历史执行回执'], ['不重新采样，不重新执行']),
    'get_report_template': (['取得可自由修改的报告草稿'], ['不填写事实，不提交报告，不制作工具']),
    'report_gap': (['提交原始报告并等待云端支持'], ['不自动修复现场，不保证工具制作成功']),
    'request_cloud_plan': (['请求高阶策略'], ['策略不直接执行现场配置']),
    'finish': (['结束当前任务并独立核验结果'], ['不补做动作，不以模型声明认定成功或资源无解']),
}


def effect_contract(name, dynamic=False):
    if dynamic == 'network-diagnostic-v1':
        return {'changes':['计算所引用观测的业务目标检查事实'],'does_not':['只读，不修改配置，不修复现场']}
    if dynamic == 'network-control-v1':
        return {'changes':['仅按已声明capabilities修改请求路由、源速率或standard-v1设备配置'],
                'does_not':['不修复物理故障，不增加容量，不绕过其他设备协议；检查工具声明的能力子集']}
    if name == 'configure_device' or dynamic:
        return copy.deepcopy(DEVICE_EFFECT)
    changes, does_not = EFFECTS.get(name, (['返回本工具契约规定的事实或状态'], ['不代表业务修复，不自动选择后续方案']))
    return {'changes': list(changes), 'does_not': list(does_not)}


def task_progress(session, action, result=None, before=None):
    """Use explicit samples or the just-executed readback, never hidden sampling.

    Executor readback has no observation_id: the agent must still sample before
    another state-bound operation. Predictions are kept outside actual facts.
    """
    if getattr(session,'appliance',False):
        from .appliance import progress
        return progress(session)
    result = result or {}
    live = result.get('scope') == 'live_simulation'
    sample = session.tools.sensor.latest
    cached = session.tools.progress_facts
    identity = session.tasks.identity()
    if live:
        facts = session.world.snapshot()
        origin = {'kind': 'executor_readback', 'action': action, 'scope': 'current_simulation_only'}
    elif cached and cached['identity'] == identity and action not in ('read_sensor_adapter', 'inspect_links', 'inspect_services'):
        facts, origin = copy.deepcopy(cached['facts']), copy.deepcopy(cached['source'])
    elif sample and all(sample.get(k) == v for k, v in session.tasks.identity().items()):
        facts = copy.deepcopy(sample)
        origin = {'kind': 'explicit_sensor_sample', 'observation_id': sample['observation_id'],
                  'sample_sequence': sample['sample_sequence'], 'observed_at': sample['observed_at']}
    else:
        return {'source': 'no_explicit_observation', 'actual_state_known': False,
                'summary': '尚无本任务主动观测；未提供现场状态或处置答案。'}
    session.tools.progress_facts = {'facts': copy.deepcopy(facts), 'source': copy.deepcopy(origin), 'identity': identity}
    goal = session.world.task
    metrics = facts['metrics']
    voice = metrics.get('voice', {}).get('demand_satisfaction')
    video = metrics.get('video', {}).get('delivered_mbps')
    checks = {'voice_target_met': voice is not None and voice >= goal['voice_min_satisfaction']-1e-9,
              'video_floor_met': video is not None and video >= goal['video_min_mbps']-1e-9}
    row = session.tasks.active()
    progress = {**session.tasks.identity(), 'source': origin, 'actual_state_known': True,
        'config_version': facts['version'], 'environment_revision': facts.get('environment_revision', 1),
        'measurement_step': facts['step'], 'task_state': row['state'] if row else None,
        'business_objective_met': all(checks.values()), 'checks': checks,
        'services': {ident: {'route': list(flow['route']),
            'source_limit_mbps': flow['limit_mbps'],
            'delivered_mbps': metrics.get(ident, {}).get('delivered_mbps'),
            'demand_satisfaction': metrics.get(ident, {}).get('demand_satisfaction')}
            for ident, flow in facts['flows'].items()},
        'fresh_observation_required_after_write': origin['kind'] == 'executor_readback',
        'note': '仅总结观测/执行事实，不是下一动作指令；已发布工具或执行成功均不等于业务达标。'}
    changed = []
    if live and before:
        for ident, link in facts['links'].items():
            delta = {key: {'before': before['links'][ident][key], 'after': link[key]}
                     for key in ('enabled', 'channel') if before['links'][ident][key] != link[key]}
            if delta:
                changed.append({'link_id': ident, 'changes': delta})
    progress['device_changes_in_this_execution'] = changed
    if result.get('scope') == 'counterfactual_snapshot_only':
        progress['prediction_is_actual'] = False
    routes = '；'.join(ident+'经过'+','.join(flow['route']) for ident, flow in facts['flows'].items())
    outcome = '业务指标已达标' if all(checks.values()) else '业务指标尚未全部达标'
    progress['summary'] = ('本次执行读回：' if live else '最近执行读回中：' if origin['kind']=='executor_readback' else '上次主动观测中：')+('设备配置已更新；' if changed else '')+routes+'；'+outcome+'。'
    return explicit_versions(progress)
