"""r15: model-selected steps, ordered native batches and local host commands.

The task graph describes protocol responsibilities; it does not prescribe the
model's solution. Host commands use the current user's privileges, not a sandbox.
"""
import copy
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
from collections import Counter

_vendor = str(Path(__file__).resolve().parents[1] / 'vendor')
if _vendor not in sys.path:
    sys.path.insert(0, _vendor)
from collab_lab.common import Evidence, write_json
from collab_lab.rag import RagLibrary
from .adapter import NodeProviders
from .catalog import CAT, EXECUTION_DESCRIPTION, current_measured_plan_options, obj, S, task_catalog
from .memory import WorkingMemory
from .policy import receipt_safe
from .validation import model_history, validate

SYSTEM_R15 = '''你是通信现场的自主智能体。A兼任中心协调者，维护共同方案；B可主动提出建议、独立接受或拒绝；双方分别执行自己的配置。
你可以自行选择解决方法、调用顺序和步骤数。任务卡和focus_tools只描述当前协议责任与建议，不限制调查、计算、通信或本地命令。
每轮可调用一个或多个工具。多个调用按你提交的顺序实际执行并分别返回回执；需要先看结果再决定的步骤请留到下一轮。失败回执不会自动取消同轮其他调用。相同工具可按需反复使用。
run_local_command可在本端实验工作环境执行系统命令、读写文件、创建和运行脚本，无命令白名单。仅操作本实验授权的环境，不修改其他项目、共享模型服务或模型权重，不进行训练、蒸馏、剪枝或量化。它以当前宿主账号权限运行。
双方资源由各自主动观测并通过消息交换。协议记录和历史测量不是新的现场观测。框架只绑定身份，不替你诊断、选参数、改方案、接受、执行或结案；本地命令成功也不等于网络配置已经执行。
共同方案需双方独立接受和有效准备，A授权后两端分别执行。方案版本、观测鲜度和节点执行权限以真实回执为准。遇到错误可自行调整工具、补采、调查、修订或报告阻塞。
主动采样、计算、审议、执行和finish可以由你自主组合。完成协作后需确认双方实际执行回执，并各自主动复采；无法完成可明确finish unresolved或needs_resources。solved/unresolved/needs_resources均是正常流程结案，业务效果单独记录。
语音最低满足率95%，视频最低2Mbps、期望5Mbps。video_rate_mbps是配置值，measurements.video_delivered_mbps是实际交付。收到消息不自动接受或执行。未知写结果先查原请求，避免重复写。
整个场景两端共享1800秒时间窗口，包含模型推理、工具执行和等待；无64步上限，无同类错误3次终止。只有持续无实质进展的重复循环才会保护性终止。
纯文字回复会保留，但不自动变成工具动作、等待或完成；要执行或结案请实际调用相应工具。finish只结束本次本地任务，真实新的协作请求仍可在原时间窗口内激活后续任务。
'''


def flexible_catalog(node):
    catalog = task_catalog()
    if node == 'B':
        for name in ('offer_group_plan', 'revise_group_plan'):
            catalog.pop(name)
    catalog['execute_group_plan'] = (EXECUTION_DESCRIPTION, catalog['execute_group_plan'][1])
    for name in ('read_local_commitments', 'get_request_status'):
        catalog[name] = copy.deepcopy(CAT[name])
    catalog['run_local_command'] = (
        '在本端实验工作环境运行系统命令，可读写文件、创建并运行脚本，无命令白名单。'
        '默认目录为本节点独立工作目录；cwd可指定实验根内目录。使用宿主账号权限，不是安全沙箱。'
        '返回真实退出码、输出、超时和日志路径，不代替网络执行回执。',
        obj({'command': S, 'cwd': S, 'timeout_seconds': {'type': 'number'}}, ['command']))
    return catalog


def stop_command(process):
    """Only stop this invocation's owned process tree on timeout."""
    if os.name == 'nt':
        subprocess.run(['taskkill', '/PID', str(process.pid), '/T', '/F'],
                       capture_output=True, timeout=10)
    else:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass


def run_local_command(root, workspace, arguments, receipt_folder, remaining):
    root, workspace = Path(root).resolve(), Path(workspace).resolve()
    cwd = Path(arguments.get('cwd') or workspace)
    if not cwd.is_absolute():
        cwd = workspace / cwd
    cwd = cwd.resolve()
    if not cwd.is_relative_to(root):
        return {'ok': False, 'error_code': 'WORKSPACE_REQUIRED',
                'error': 'cwd must belong to this isolated experimental runtime'}
    if remaining <= 0:
        return {'ok': False, 'error_code': 'SCENARIO_DEADLINE'}
    requested = arguments.get('timeout_seconds', remaining)
    if type(requested) not in (int, float) or requested <= 0:
        return {'ok': False, 'error_code': 'INVALID_ARGUMENTS', 'error': 'timeout_seconds must be positive'}
    timeout = min(requested, remaining)
    cwd.mkdir(parents=True, exist_ok=True)
    receipt_folder = Path(receipt_folder)
    receipt_folder.mkdir(parents=True, exist_ok=True)
    stdout_path, stderr_path = receipt_folder / 'stdout.txt', receipt_folder / 'stderr.txt'
    command = (['powershell.exe', '-NoProfile', '-NonInteractive', '-Command', arguments['command']]
               if os.name == 'nt' else ['/bin/bash', '-c', arguments['command']])
    started = time.monotonic()
    try:
        options = {'creationflags': subprocess.CREATE_NEW_PROCESS_GROUP} if os.name == 'nt' else {'start_new_session': True}
        with stdout_path.open('wb') as stdout, stderr_path.open('wb') as stderr:
            process = subprocess.Popen(command, cwd=cwd, stdout=stdout, stderr=stderr, **options)
            timed_out = False
            try:
                process.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                timed_out = True
                stop_command(process)
                process.wait(timeout=10)
        outputs = {}
        for label, path in (('stdout', stdout_path), ('stderr', stderr_path)):
            with path.open('rb') as stream:
                size = path.stat().st_size
                stream.seek(max(0, size - 65536))
                outputs[label] = stream.read().decode('utf-8', errors='replace')
            outputs[label + '_path'] = str(path)
            outputs[label + '_truncated_in_model_view'] = size > 65536
        result = {'ok': not timed_out and process.returncode == 0,
                  'result': {'returncode': process.returncode, 'timed_out': timed_out,
                             'cwd': str(cwd), 'seconds': time.monotonic() - started, **outputs}}
        if timed_out:
            result['error_code'] = 'COMMAND_TIMEOUT'
        elif process.returncode:
            result['error_code'] = 'COMMAND_EXIT_NONZERO'
    except OSError as error:
        result = {'ok': False, 'error_code': 'COMMAND_START_FAILED', 'error': str(error)}
    write_json(receipt_folder / 'receipt.json', {'model_arguments': arguments, 'response': result})
    return result


def progress_context(context):
    """Exclude message IDs, sample IDs and TTL refreshes from progress identity."""
    plan = context.get('current_plan') or {}
    result = {'observation': context.get('local_observation_valid'),
            'peer_resource_snapshot_available': context.get('peer_resource_snapshot_available'),
            'plan': {key: plan.get(key) for key in ('plan_hash', 'accepted', 'authorized', 'applied')},
            'prepared_nodes': sorted(plan.get('ready', {})),
            'channel_available': context.get('channel_available')}
    if 'peer_task_exit' in context:result['peer_task_exit']=context['peer_task_exit']
    return result


class ProgressGuard:
    def __init__(self, limit=20):
        self.limit, self.context, self.visits = limit, None, Counter()

    def before(self, action, arguments, context):
        state = json.dumps(context, sort_keys=True, ensure_ascii=False)
        if state != self.context:
            self.context, self.visits = state, Counter()
        useful = {k: v for k, v in arguments.items() if k not in ('reason', 'explanation', 'observation_id', 'request_id')}
        key = json.dumps([action, useful], sort_keys=True, ensure_ascii=False)
        self.visits[key] += 1
        return self.visits[key] < self.limit


def run_flexible_job(args, client, job):
    cid, node = job['group_task_id'], args.node
    revision = job.get('task_revision', 'r15')
    activation = job.get('activation', 1)
    folder = args.root / 'runs' / cid / node
    if activation > 1:
        folder /= 'activation_' + str(activation)
    ev = Evidence(folder)
    ev.add('node_identity', node, {'hostname': __import__('socket').gethostname(),
                                 'node_id': node, 'boot_id': args.boot, 'group_task_id': cid,
                                 'activation': activation, 'revision': revision})
    base = {'case_id': cid, 'boot_id': args.boot}
    if not client.call({'op': 'register', **base}).get('registered'):
        raise RuntimeError('node registration rejected')
    started = time.monotonic()
    deadline = started + min(1800, job.get('remaining_seconds', 1800))
    remaining = lambda: max(0, deadline - time.monotonic())
    config = {'models': {'edge': {'model': 'gemma4-e2b:latest', 'base_url': args.model_url}},
              'edge_context': 65536, 'edge_output_tokens': 16384, 'edge_output_tokens_max': 32768,
              'edge_length_retry': True, 'edge_thinking': True, 'edge_request_timeout_seconds': 900,
              'request_timeout_seconds': 180, 'response_retry_attempts': 1,
              'transport_retry_attempts': 1, 'native_multi_call': True,
              'absolute_deadline_monotonic': deadline}
    provider = NodeProviders(config)
    library = RagLibrary(args.root / 'vendor/knowledge', strategy_pack=args.root / 'vendor/strategies')
    memory = WorkingMemory(args.root / 'runs' / cid / node / 'working_memory.json', cid, node)
    workspace = args.root / 'agent_workspaces' / cid / node
    workspace.mkdir(parents=True, exist_ok=True)
    initial = {'group_task_id': cid, 'node_id': node, 'goal': job['goal'], 'cloud_available': False,
               'workspace': str(workspace), 'time_budget_seconds': remaining(), 'max_decisions': None}
    if memory.path.exists():
        initial['working_memory'] = memory.context()
    system = SYSTEM_R15
    if receipt_safe(job):
        system += ('\n本版本区分本端退出与共同方案完成。unresolved/needs_resources无需新的有效观测即可实际提交，缺失或过期观测会如实记录。'
                   'solved仍需真实观测；已接受的本端片段尚未执行时不能报告共同任务完成。'
                   'node_task_closed仅表示对端本次任务退出，不是执行回执；尚未履行的共同承诺仍会显示。'
                   '可自行调查、发送协作请求接续、调整未授权方案或明确退出。退出通知本身不会反复唤醒已经结束的任务。')
    messages = [{'role': 'system', 'content': system + '\n你的节点身份：' + node},
                {'role': 'user', 'content': json.dumps(initial, ensure_ascii=False)}]
    cursor, lastobs, step, group_calculation, sample_facts = 0, None, 0, None, {}
    status, guard = 'observation_window_ended', ProgressGuard()
    prefix = f'{args.boot}:{cid}:activation:{activation}'
    if activation > 1:
        packet = job.get('activation_inbox') or {}
        rows = packet.get('messages', [])
        source = (job.get('activation_source') or {}).get('source_message_id')
        if packet.get('activation') != activation or not any(m['message_id'] == source for m in rows):
            raise RuntimeError('SOURCE_DELIVERY_REQUIRED: new task requires actual source packet')
        memory.record_peer(rows, activation)
        messages.append({'role': 'user', 'content': json.dumps({'peer_events': rows}, ensure_ascii=False)})
        cursor = packet['cursor']
        ev.add('peer_messages_delivered_to_model', node, {'message_ids': [m['message_id'] for m in rows], 'messages': rows, 'source': 'assignment_delivery'})
    catalog = flexible_catalog(node)
    config['tool_descriptions'] = {name: spec[0] for name, spec in catalog.items()}
    while remaining() > 0:
        inbox = client.call({'op': 'tool', **base, 'name': 'read_peer_events', 'arguments': {'after_sequence': cursor}, 'request_id': prefix + ':inbox:' + str(step)})
        if inbox.get('ok') and inbox['result']['messages']:
            rows, cursor = inbox['result']['messages'], inbox['result']['cursor']
            memory.record_peer(rows, activation)
            messages.append({'role': 'user', 'content': json.dumps({'peer_events': rows}, ensure_ascii=False)})
            ev.add('peer_messages_delivered_to_model', node, {'message_ids': [m['message_id'] for m in rows], 'messages': rows})
        context = client.call({'op': 'runtime_state', **base, 'observation_id': lastobs})
        deadline = min(deadline, time.monotonic() + context.get('remaining_seconds', remaining()))
        packet = {'protocol_context': {k: v for k, v in context.items() if k != 'plan_calculation_sources'},
                  'task_card_is_advisory': True, 'remaining_seconds': remaining()}
        measured = current_measured_plan_options(context.get('plan_calculation_sources'), group_calculation)
        if measured is not None:
            packet['observed_common_plan_options'] = measured
        model_messages = model_history(messages) + [{'role': 'user', 'content': json.dumps(packet, ensure_ascii=False)}]
        try:
            answer = provider.call('edge', model_messages, {k: v[1] for k, v in catalog.items()}, ev)
        except Exception:
            if remaining() <= 0:
                ev.add('scenario_deadline_during_inference', node, {'tool_executed': False})
                break
            raise
        actions = (answer['parsed'] or {}).get('actions', [])
        client.call({'op': 'trace', **base, 'call_id': answer['call_id'], 'action': 'batch' if actions else 'text_response',
                     'actions': [a.get('action') for a in actions], 'hostname': __import__('socket').gethostname(),
                     'input_sha256': hashlib.sha256(json.dumps(model_messages, sort_keys=True, ensure_ascii=False).encode()).hexdigest(),
                     'peer_message_ids': [m['message_id'] for row in model_messages if row['role'] == 'user' for m in json.loads(row['content']).get('peer_events', [])]})
        messages.append(answer['assistant_message'])
        if remaining() <= 0 and not actions:
            break
        if not actions:
            ev.add('text_response_without_tool', node, {'call_id': answer['call_id'], 'tool_executed': False, 'automatic_finish': False})
            if not guard.before('no_action', {}, {**progress_context(context), 'sample': sample_facts}):
                status = 'no_progress_loop'
                break
            messages.append({'role': 'user', 'content': json.dumps({'runtime_receipt': {'tool_calls_received': 0, 'task_finished': False}})})
            step += 1
            continue
        # Bind the plan the model actually saw. Only an earlier successful
        # proposal in this very batch can supply a newly created identity.
        plan_binding = {'plan_id': context.get('current_plan_id'), 'expected_plan_hash': context.get('current_plan_hash')}
        stopped, deferred_messages = False, []
        for index, value in enumerate(actions):
            action, original = value.get('action'), value.get('arguments')
            rid = f'{prefix}:decision:{step}:tool:{index}'
            effective = copy.deepcopy(original) if isinstance(original, dict) else {}
            state = client.call({'op': 'runtime_state', **base, 'observation_id': lastobs})
            deadline = min(deadline, time.monotonic() + state.get('remaining_seconds', remaining()))
            error = value.get('submission_error') or (validate(original, catalog[action][1]) if action in catalog else 'unknown tool')
            if stopped or remaining() <= 0:
                result = {'ok': False, 'error_code': 'BATCH_NOT_EXECUTED', 'error': 'local task ended, loop detected or shared deadline reached'}
            elif error:
                result = {'ok': False, 'error_code': 'INVALID_ARGUMENTS', 'error': error}
            elif not guard.before(action, original, {**progress_context(state), 'sample': sample_facts}):
                result = {'ok': False, 'error_code': 'NO_PROGRESS_LOOP', 'error': '20 repeated actions without substantive state progress'}
                status, stopped = 'no_progress_loop', True
            else:
                effective.setdefault('observation_id', lastobs)
                if action == 'wait_for_collaboration':
                    effective['after_sequence'] = cursor
                if action in ('review_group_plan', 'revise_group_plan', 'execute_group_plan', 'read_group_plan'):
                    effective.update(plan_binding)
                if action == 'retrieve_knowledge':
                    result = {'ok': True, 'result': library.search(original['query'])}
                elif action == 'get_report_template':
                    result = {'ok': True, 'result': json.loads((args.root / 'vendor/templates/capability_gap_report.json').read_text(encoding='utf-8-sig'))}
                elif action == 'report_gap':
                    ev.add('original_gap_report', node, {'report': original['report']})
                    result = {'ok': False, 'error_code': 'CLOUD_UNAVAILABLE', 'error': 'Cloud support disabled for base group; report preserved unchanged'}
                elif action == 'run_local_command':
                    result = run_local_command(args.root, workspace, original, folder / 'local_commands' / f'{step}_{index}', remaining())
                    client.call({'op': 'local_tool_receipt', **base, 'request_id': rid, 'name': action, 'arguments': original, 'response': result})
                elif action == 'get_request_status':
                    result = client.call({'op': 'request_status', **base, 'request_id': original['request_id']})
                else:
                    result = client.call({'op': 'tool', **base, 'name': action, 'arguments': effective,
                                          'model_arguments': original, 'request_id': rid,
                                          'task_context_token': state['task_dispatch']['context_token']})
            ev.add('framework_identity_binding', node, {'model_arguments': original, 'effective_arguments': effective, 'business_values_rewritten': False, 'batch_index': index})
            ev.add('node_tool_result', node, {'action': action, 'model_arguments': original, 'arguments': effective, 'request_id': rid, 'response': result, 'call_id': answer['call_id'], 'batch_index': index})
            if isinstance(original, dict):
                memory.record_tool(action, original, effective, result, rid, activation)
            tool_message = {'role': 'tool', 'tool_name': action or 'invalid_call', 'content': json.dumps({'request_id': rid, 'tool_result': result}, ensure_ascii=False)}
            if 'tool_call_id' in value:
                tool_message['tool_call_id'] = value['tool_call_id']
            messages.append(tool_message)
            if result.get('ok'):
                if action == 'read_sensor_adapter':
                    lastobs = result['result']['observation_id']
                    sample_facts = {k: result['result'][k] for k in ('config_version', 'environment_revision', 'ports', 'selected_path', 'video_rate_mbps', 'measurements')}
                elif action == 'calculate_link_options' and original.get('scope') == 'group':
                    group_calculation = result['result']
                elif action in ('offer_group_plan', 'revise_group_plan'):
                    plan = result['result']['plan']
                    plan_binding = {'plan_id': plan['plan_id'], 'expected_plan_hash': plan['plan_hash']}
                elif action == 'finish':
                    status, stopped = original['status'], True
                elif action == 'wait_for_collaboration':
                    from .agent import await_peer
                    wait = await_peer(client, base, cursor, answer['call_id'], min(180, remaining()), register=False)
                    deferred_messages.append({'role': 'user', 'content': json.dumps({'wait_result': wait}, ensure_ascii=False)})
                    if wait.get('state') == 'ready':
                        cursor = wait['cursor']
                        memory.record_peer(wait['messages'], activation)
                        deferred_messages.append({'role': 'user', 'content': json.dumps({'peer_events': wait['messages']}, ensure_ascii=False)})
                        ev.add('peer_messages_delivered_to_model', node, {'message_ids': [m['message_id'] for m in wait['messages']], 'messages': wait['messages'], 'source': 'explicit_wait'})
        messages.extend(deferred_messages)
        step += 1
        write_json(folder / 'progress.json', {'model_turns': step, 'status': status if stopped else 'running', 'remaining_seconds': remaining()})
        if stopped:
            break
    if status not in ('solved', 'unresolved', 'needs_resources'):
        client.call({'op': 'runtime_end', **base, 'reason': status})
    memory.record_outcome(status, activation)
    write_json(folder / 'task_graph.json', client.call({'op': 'task_snapshot', **base}))
    write_json(folder / 'result.json', {'status': status, 'elapsed_seconds': time.monotonic() - started,
                                      'model_calls': provider.counter, 'node_id': node, 'revision': revision,
                                      'scenario_window_seconds': 1800, 'model_decision_limit': None})
