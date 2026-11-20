def obj(props,required=None):return {'type':'object','properties':props,'required':list(props) if required is None else required,'additionalProperties':False}
S={'type':'string'};NUM={'type':'number'};BOOL={'type':'boolean'}
ID={'observation_id':S};PLAN={'plan_id':S}
FRAGMENT=obj({'path':{'type':'string','enum':['primary','backup']},'channel':{'type':'string','enum':['c1','c2']},'video_rate_mbps':NUM})
CAT={
 'read_sensor_adapter':('主动读取本节点接口、资源、配置、业务原始测量；不会给出对端私有状态或异常结论。',obj({})),
 'record_finding':('记录模型自己依据观测作出的发现；不替模型判定。',obj({**ID,'has_problem':BOOL,'problem_type':S})),
 'read_peer_catalog':('获取对端身份和能力；没有实时资源读数。',obj({})),
 'request_peer_resources':('明确请求对端发布其主动观测的资源快照；对端仍自行决定读取和发布，不自动替对端采样。',obj({'reason':S},[])),
 'wait_for_collaboration':('明确声明等待对端资源、方案变更、授权或执行回执；只有真实新消息才唤醒，无新消息不重新生成答案。',obj({'awaited':{'type':'string','enum':['peer_resources','plan_update','authorization','execution_receipt']}})),
 'read_local_commitments':('读取本端持久协议账本：本端接受/准备、已知授权、本端是否实际执行及最近一次执行回执。对端执行不等于本端执行。无配置动作。',obj({})),
 'publish_resource_snapshot':('将本端最近主动观测的原始资源快照发布给对端，供其自行计算或协商；不读取对端私有状态，不接受/执行方案。',obj(ID)),
 'calculate_link_options':('依据最近本端观测枚举全部端口的资源可行速率范围，扣除语音需求和背景占用。scope=local只检查本端；scope=group还需已实际收到的对端资源快照。返回全部选项，不选方案、不配置；实测可能变化，准备和执行仍核验版本。',obj({**ID,'scope':{'type':'string','enum':['local','group']}})),
 'send_peer_message':('发送给对端的事实/问题/提议。requires_response=true表示新的协作请求，可唤醒已完成普通任务的对端；不执行对端操作。',obj({'payload':{'type':'object','additionalProperties':True},'requires_response':BOOL},['payload'])),
 'read_peer_events':('读取对端已经发送的消息与计划通知；消息不自动执行。',obj({'after_sequence':{'type':'integer'}},[])),
 'wait_peer_event':('有界等待对端事件；休眠10秒再读，减少无进展推理。',obj({'after_sequence':{'type':'integer'}},[])),
 'configure_local_rate':('仅修改本节点视频速率上限2..5Mbps；不切换链路，不修改对端。',obj({**ID,'video_rate_mbps':NUM})),
 'offer_group_plan':('提出双方配置片段并准备本端资源。每节点path/channel/video_rate_mbps；两端路径和信道须一致。提议者接受自己的方案；对端仍须自主审议。不写配置。回执保留计划ID及部分完成步骤。',obj({**ID,'node_actions':obj({'A':FRAGMENT,'B':FRAGMENT})})),
 'revise_group_plan':('用模型重新选择的双方配置替换指定未授权方案，并准备本端。旧方案立即失效，新方案仅修订者接受，对端必须重新独立审议。不能替换已授权或部分执行方案。不自动选择路径或速率。',obj({**ID,**PLAN,'node_actions':obj({'A':FRAGMENT,'B':FRAGMENT})})),
 'review_group_plan':('依据本机观测自主接受或拒绝指定计划。接受时一并准备本端资源；拒绝时不准备。不写配置，可重新审议以刷新准备。',obj({**ID,**PLAN,'accept':BOOL,'explanation':S},['observation_id','plan_id','accept'])),
 'read_group_plan':('读取指定共同计划的双方接受、准备、授权和执行回执状态。',obj(PLAN)),
 'execute_group_plan':('自主决定执行指定共同计划的本端片段。A调用时先检查双方接受/有效准备并授权，再只执行A；B只能执行已获A授权且自身准备有效的B片段。对端必须自己另行调用。成功后主动复采。',obj({**ID,**PLAN})),
 'get_request_status':('查询自己请求的持久回执；unknown时不得盲重放写操作。',obj({'request_id':S})),
 'retrieve_knowledge':('检索本节点固定知识/策略资料；仅参考，适用性由模型判断。',obj({'query':S})),
 'get_report_template':('获取可编辑的能力缺口报告，字段允许增删。',obj({})),
 'report_gap':('报告真实能力缺口；基础组云端禁用，云端支持以实际回执为准。',obj({'report':{'type':'object','additionalProperties':True}})),
 'finish':('结束当前本地任务，须完成任务规定的本端采样窗口。选择solved/unresolved/needs_resources并引用最新观测。对端的新协作请求仍可激活新任务。',obj({**ID,'status':{'type':'string','enum':['solved','unresolved','needs_resources']}})),
}
SYSTEM='''你是通信现场的端侧自主智能体，使用本机工具观察、分析、处置和核验，按需与另一个智能体协作。
你只控制本节点；A兼固定协调角色，B可主动发起协作并有权拒绝资源安排。两端角色均有独立决策权。
持续调查业务是否满足既定目标；需主动读取传感器数据并完成任务指定的本端采样窗口。对端可能较慢，可有界等待事件。
跨端配置通过三个语义操作：offer_group_plan提出方案并准备本端；review_group_plan由对端自主接受并准备或拒绝；execute_group_plan由双方各自决定执行本端。A执行时检查双方准备并授权，B执行时要求A已授权。提出和接受都不写配置，收到消息不意味着必须照做。
每次配置后须重新主动采样再继续配置或结束。过期观测/准备以工具真实拒绝为准，你自己决定后续调查或修订。未知写结果先查询原请求，不盲重放。
finish只结束当前本地任务；服务继续接收新的协作提案，并以新的本地任务激活。所有任务共用本场景的决策和时间预算；失败任务不会因消息自动重开。
模型自己判断是否异常、如何组合工具、是否协作、是否结束；可调用record_finding记录判断。知识仅参考，不覆盖当前真实观测。
业务目标为语音满足率至少95%、视频至少2Mbps，期望5Mbps；视频可在2..5内降级。不要因工具返回成功就声称任务完成。
工具目录及参数是公开通用契约，不以文字或手写action JSON替代工具调用。调用封装与请求身份由框架处理。
需要业务操作时每轮调用一个工具。没有业务动作的正常停止进入待命，不等于任务完成；只有新的对端消息会再次唤醒推理，不会定时重问同一输入。双方待命且没有待交付消息，或对端已结束且无新消息时，本轮由运行窗口收尾，不能计为模型solved。
资源准备失败的真实回执会传给对端，接受意图不等于已经准备。你可自行调查、沟通或调用revise_group_plan修订；新方案必须重新征得对端同意，被替代方案不能执行。
每次推理的protocol_context是本端持久协议回执，不是新的传感器观测。local_applied只表示本端实际执行；对端执行不能代替它。未完成的已授权承诺跨普通任务保留，实际执行仍由本端自主决定，并须先主动读取当前任务观测。新任务本身不撤销未过期且状态版本未变化的准备。
可以主动使用calculate_link_options计算所有局部资源选项；publish_resource_snapshot仅分享本端已观测事实，收到对端快照后可计算group选项。计算结果不是已执行配置，也不替你选择方案。
'''

# Observation/cursor identities are transport metadata, not a handwritten JSON exam.
for _name,(_description,_schema) in list(CAT.items()):
    if 'observation_id' in _schema.get('properties',{}):
        _schema['properties'].pop('observation_id')
        _schema['required'].remove('observation_id')
        CAT[_name]=(_description+' 观测身份由框架绑定最近一次主动采样；未采样或过期会拒绝。',_schema)

STRUCTURED_SYSTEM='''你是通信现场端侧自主智能体。A维护当前共同方案及其唯一修订链，B负责本端资源意见；B可主动请求协作、提出修改建议、接受或拒绝方案。两端均只执行自己的片段。
公开工具名和参数契约保持稳定。protocol_context.available_tools说明当前具备前提的工具，allowed_wait_targets说明当前合法等待目标；不满足前提或参数非法时返回真实错误回执，不执行、不修正参数。你自主决定如何调查、选择配置、接受或拒绝、执行与核验；框架只管理前置条件、身份、消息及当前方案版本，不选择业务参数。
主动传感器观测才是现场事实。protocol_context是协议记录，current_plan是共享控制记录，不是新的传感器读数。local_applied仅代表本端执行，其他节点的回执不能替代它。
组级资源计算及A提出方案需要实际收到的对端资源快照；可request_peer_resources请求，节点自行读取后publish_resource_snapshot回应。calculate_link_options列出全部已知可行区间，不替你选择或执行。B可用send_peer_message发起协作或提出修改意见。
一个时刻只有一个共同方案：A提出或修订，双方各自审议并准备；A明确执行时核验双方准备并授权，只执行A；B收到授权后自行执行B。准备失败不等于协议接受。当前方案身份由框架固定到你本轮看到的版本，变化会拒绝，不能猜ID或替换业务参数。
最低保障目标是语音满足率至少95%、视频至少2Mbps。语音满足率=voice_delivered_mbps/voice_demand_mbps；视频单位Mbps。期望视频5Mbps属于质量目标，最低保障恢复与期望质量分别记录。
finish solved表示已主动观测核验最低保障达到；quality_status可报告desired_reached/degraded_acceptable/not_assessed。protocol_disposition可报告completed/deferred/abandoned/not_required。结束本地任务不代表共同方案双方执行完成；采样覆盖由观察器另行记录。配置后旧观测失效，继续配置或finish前必须主动复采。
等待时明确调用wait_for_collaboration并说明awaited。正常停止却不调用工具属于未提交动作，不会自动替你声明等待或任务完成。每轮只选一个原生工具，工具成功不等于业务成功，消息不自动接受/执行方案。
知识仅参考。你可依据事实结束unresolved或needs_resources；不需要为了得到成功结论继续无意义操作。
本端local_work记录调查与协议责任，不给出故障结论或配置答案。资源请求前须有本端有效观测。resource_response_owed=true表示你欠对端回执：对端已向你请求资源，而你尚未发布回应；此时新请求和等待不可用，你可自行观测、发布或结束。outgoing_resource_request_ids表示你发出的请求尚未收到对端回复，和你欠对端回执是两回事；你发布了本端资源且没有欠回执后，可按allowed_wait_targets等待真实外部回复。
等待只针对allowed_wait_targets中的实际对端责任：等资源须已有未获回应的请求并发布当前本端资源；B发布资源后可等A的当前方案，A须自行决定共同方案，不能等待B创建；接受后的B可等A授权，已执行本端后可等对端执行回执。目录未开放等待时，你仍可调查、沟通、报告缺口或结束，框架不替你完成任何业务动作。
video_rate_mbps是本地配置上限，measurements.video_delivered_mbps才是实际交付。NO_CHANGE表示没有写入，不能据此认定业务恢复。finish状态仍由你依据实际观测决定，原始判断会独立核对。
working_memory仅保存本端过去实际收到的消息和工具回执，模型原参数仍是原判断。历史观测/计算没有重新核验鲜度，不能当成本任务新观测；写入身份仍只绑定本任务最近主动采样。
'''

STRUCTURED_TOOL_NAMES=(
    'read_sensor_adapter','record_finding','request_peer_resources','publish_resource_snapshot',
    'calculate_link_options','send_peer_message','configure_local_rate','offer_group_plan',
    'revise_group_plan','review_group_plan','read_group_plan','execute_group_plan',
    'wait_for_collaboration','retrieve_knowledge','get_report_template','report_gap','finish',
)

def _structured_definitions(names):
    import copy
    result={name:(description,copy.deepcopy(schema)) for name in names for description,schema in [CAT[name]]}
    for name in ('review_group_plan','revise_group_plan','execute_group_plan','read_group_plan'):
        if name not in result:continue
        description,schema=result[name]
        schema['properties'].pop('plan_id',None)
        if 'plan_id' in schema['required']:schema['required'].remove('plan_id')
        result[name]=('作用于本轮protocol_context中的当前方案；方案ID/hash由框架绑定，变化会拒绝。'+description,schema)
    if 'finish' in result:
        description,schema=result['finish'];schema['properties'].update(quality_status={'type':'string','enum':['desired_reached','degraded_acceptable','not_assessed']},protocol_disposition={'type':'string','enum':['completed','deferred','abandoned','not_required']})
        result['finish']=('结束本地任务；solved对应主动观测已核验最低保障；期望质量quality_status和协议protocol_disposition分别报告。至少一次当前有效观测即可提交，采样次数另计分；执行后须主动复采。',schema)
    return result

def structured_catalog(state):
    """Retain the r9 catalog for archived contract regression checks."""
    result=_structured_definitions(state['available_tools'])
    if 'calculate_link_options' in result and not state['peer_resource_snapshot_available']:
        result['calculate_link_options'][1]['properties']['scope']['enum']=['local']
    if 'wait_for_collaboration' in result:
        result['wait_for_collaboration'][1]['properties']['awaited']['enum']=list(state['allowed_wait_targets'])
    return result

def stable_catalog():
    """Public schemas stay fixed; the authenticated dispatcher checks readiness."""
    return _structured_definitions(STRUCTURED_TOOL_NAMES)

def task_catalog():
    """One fixed r11 contract; archived r10 schemas remain available unchanged."""
    result=stable_catalog()
    result['finish']=('依据本端实际观测结束任务，报告solved/unresolved/needs_resources；协议进度由真实回执账本记录。无法完成核验时可明确提交unresolved/needs_resources，原判断原样保留。',obj({'status':{'type':'string','enum':['solved','unresolved','needs_resources']}}))
    return result

EXECUTION_DUTY_GUIDANCE = (
    '当前职责是处理尚无本端执行回执的片段。依据本端观测、准备和授权，自主决定是否可以执行。'
    '选择执行时，提交本端执行并以实际回执为准。不能执行时可调查或具体说明障碍。'
    '对端已经执行、当前业务达标或口头确认都不能代替本端回执。')
EXECUTION_DESCRIPTION = (
    '仅当你依据当前本端观测和授权独立决定可以执行时，提交本端执行。'
    '本工具不接收任何参数，调用对象必须是空对象 {}。'
    '框架会按当前轮的方案和版本绑定身份；不要传入 plan_id、plan_hash、expected_plan_hash 或 observation_id。'
    '实际执行以后以工具回执为准。')


def measured_plan_options(rows):
    return {
        'basis': '当前本端采样与已收到的对端快照对应的原组级计算。',
        'meaning': '区间为 null 表示该路径在这份观测中无法保障最低业务。'
                   '若选择提案或修订，请自行选共同路径、信道与区间内的速率；也可以继续调查或决定退出。',
        'paths': [{key: row[key] for key in ('path', 'video_rate_range_mbps', 'configurable_common_channels')}
                  for row in rows],
    }


def current_measured_plan_options(sources, receipt):
    if not sources or not receipt:
        return None
    receipt_ids = {node: source['observation_id'] for node, source in receipt['observation_sources'].items()}
    if receipt_ids != sources:
        return None
    return measured_plan_options(receipt['options'])


def ready_task_catalog(context, *, explicit_execution=False):
    """Expose the current protocol tools with their original public schemas."""
    available = set(context['available_tools'])
    catalog = {name: definition for name, definition in task_catalog().items() if name in available}
    if explicit_execution and 'execute_group_plan' in catalog:
        catalog['execute_group_plan'] = (EXECUTION_DESCRIPTION, catalog['execute_group_plan'][1])
    return catalog

TASK_SYSTEM='''你是通信现场端侧自主智能体，主动观察、分析、选择配置、执行和核验。A维护唯一当前共同方案及修订链，B可主动发起协作、建议修改、独立接受或拒绝；两端均只控制自己的执行器。
protocol_context.task_dispatch.current_task是调度器依据本端协议记录分派的当前子任务，包含依赖、完成凭据和重点工具。它不含业务答案。优先处理该职责，仍由你选择工具、业务参数、接受/拒绝及业务结束状态；你可明确报告无法完成。focus_tools仅提示重点，公开17工具名和参数契约在本轮保持稳定。
只有read_sensor_adapter是主动现场观测；protocol_context、working_memory和共享计划是协议或历史记录，不是新的测量。历史观测不自动绑定当前写入；配置后须主动复采。不得把video_rate_mbps配置上限当成measurements.video_delivered_mbps实际交付。
组级计算和共同方案需要实际收到的对端资源快照；可请求资源，自主读取并发布资源回应。计算工具列出全部资源选项，不替你选路径、信道或速率。知识仅参考，可主动记录自己的异常判断。
A提出或修订未授权的方案；B自主审议并准备或拒绝；A明确execute时核验双方准备、授权并仅执行A；B收到授权后自行execute本端。当前任务和方案身份绑定到你实际看到的版本，变化返回真实拒绝，不替换参数或转向新方案。
调度器在真实依赖未满足且没有就绪工作时等待，期间不调用模型；新事件或实际超时后再分派。不得将无动作视为等待或完成。每轮只调用一个原生工具，不以手写action JSON代替。
协作执行后的核验需要先确认双方执行回执，再分别主动复采。缺回执不当作成功；真实失败、无法核验或资源不足时可明确结束unresolved/needs_resources，原判断和未完成协议步骤分别留档。
最低业务目标为语音满足率至少95%、视频至少2Mbps，期望5Mbps；语音满足率=voice_delivered_mbps/voice_demand_mbps。finish solved表示你依据实际观测判断最低保障已经达到；框架不会修正你的业务判断。结案只填写status，协议是否完成由回执账本记录。工具成功不等于业务恢复。
所有子任务及新事件激活共享每节点64次决策、整例3600秒预算。相同子任务同一动作和业务参数的同类错误最多3次；无动作和失败如实留档，不会换任务编号追问相同输入。
'''

TASK_SYSTEM_R12=TASK_SYSTEM+'''
当前任务的重点说明在每轮最后输入current_assignment。先完成其中的真实前置，再处理后继任务；公开17工具契约不变，重点列表仅含本端当前具备前提的工具。B通过send_peer_message提出协作建议或请求A提案，offer_group_plan/revise_group_plan只属于A。
requires_observation_before_finish=true时必须先自行调用read_sensor_adapter；历史测量、旧任务的solved和协议记录均不能代替本次主动观测。等待失败后的补采只用于本端调查或明确退出，不代表双方执行回执齐备。eligible_finish_statuses仅反映协议前置是否允许提交，业务状态仍由你依据事实选择。
单个子任务重复错误不会抹去历史失败。后续确有可交付的新控制消息时，可在原总预算内接续新的任务；不会因无动作或同一错误定时重问相同输入。
'''
