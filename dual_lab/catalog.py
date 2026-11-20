def obj(props,required=None):return {'type':'object','properties':props,'required':list(props) if required is None else required,'additionalProperties':False}
S={'type':'string'};NUM={'type':'number'};BOOL={'type':'boolean'}
ID={'observation_id':S};PLAN={'plan_id':S}
FRAGMENT=obj({'path':{'type':'string','enum':['primary','backup']},'channel':{'type':'string','enum':['c1','c2']},'video_rate_mbps':NUM})
CAT={
 'read_sensor_adapter':('主动读取本节点接口、资源、配置、业务原始测量；不会给出对端私有状态或异常结论。',obj({})),
 'record_finding':('记录模型自己依据观测作出的发现；不替模型判定。',obj({**ID,'has_problem':BOOL,'problem_type':S})),
 'read_peer_catalog':('获取对端身份和能力；没有实时资源读数。',obj({})),
 'send_peer_message':('发送给对端的事实/问题/提议。字段可自由组织；不执行对端操作。',obj({'payload':{'type':'object','additionalProperties':True}})),
 'read_peer_events':('读取对端已经发送的消息与计划通知；消息不自动执行。',obj({'after_sequence':{'type':'integer'}},[])),
 'wait_peer_event':('有界等待对端事件；休眠10秒再读，减少无进展推理。',obj({'after_sequence':{'type':'integer'}},[])),
 'configure_local_rate':('仅修改本节点视频速率上限2..5Mbps；不切换链路，不修改对端。',obj({**ID,'video_rate_mbps':NUM})),
 'propose_group_plan':('提出双方配置片段，每节点path/channel/video_rate_mbps；两端路径和信道须一致。提议者视为接受自己提案，对端须自主接受。',obj({**ID,'node_actions':obj({'A':FRAGMENT,'B':FRAGMENT})})),
 'respond_group_plan':('依据最新本机观测接受或拒绝计划，解释原因；不写配置。',obj({**ID,**PLAN,'accept':BOOL,'explanation':S},['observation_id','plan_id','accept'])),
 'read_group_plan':('读取指定共同计划的双方接受、准备、授权和执行回执状态。',obj(PLAN)),
 'prepare_local_plan':('已接受后，核验本机资源并预留；不执行。需最新主动观测。',obj({**ID,**PLAN})),
 'authorize_group_plan':('仅A协调角色可调用；只有双方同一计划已接受并有效准备才授权。不会代替任一端执行。',obj(PLAN)),
 'apply_local_plan':('仅执行本节点已授权且准备仍有效的片段；对端需自己调用。成功后须主动复采。',obj({**ID,**PLAN})),
 'get_request_status':('查询自己请求的持久回执；unknown时不得盲重放写操作。',obj({'request_id':S})),
 'retrieve_knowledge':('检索本节点固定知识/策略资料；仅参考，适用性由模型判断。',obj({'query':S})),
 'get_report_template':('获取可编辑的能力缺口报告，字段允许增删。',obj({})),
 'report_gap':('报告真实能力缺口；基础组云端禁用，云端支持以实际回执为准。',obj({'report':{'type':'object','additionalProperties':True}})),
 'finish':('结束自己的任务，至少双方各四次主动观测后才允许结束。选择solved/unresolved/needs_resources并引用最新观测。',obj({**ID,'status':{'type':'string','enum':['solved','unresolved','needs_resources']}})),
}
SYSTEM='''你是通信现场的端侧自主智能体，使用本机工具观察、分析、处置和核验，按需与另一个智能体协作。
你只控制本节点；A兼固定协调角色，B可主动发起协作并有权拒绝资源安排。两端角色均有独立决策权。
持续调查业务是否满足既定目标；需主动读取连续传感器数据，观察窗口为双方各至少4次主动采样。对端可能较慢，可有界等待事件。
已有工具能解决时自主组织；跨端配置需共同计划：提议/接受，双方各准备，A选择授权，然后双方各自执行。提议、接受、准备、授权均不等于实际执行。收到消息不意味着必须照做。
每次配置后须重新主动采样再继续配置或结束。过期观测/准备以工具真实拒绝为准，你自己决定后续调查或修订。未知写结果先查询原请求，不盲重放。
模型自己判断是否异常、如何组合工具、是否协作、是否结束；可调用record_finding记录判断。知识仅参考，不覆盖当前真实观测。
业务目标为语音满足率至少95%、视频至少2Mbps，期望5Mbps；视频可在2..5内降级。不要因工具返回成功就声称任务完成。
工具目录及参数是公开通用契约。只输出一个action及arguments，reason可选；调用封装与请求身份由框架处理。
'''

# Observation/cursor identities are transport metadata, not a handwritten JSON exam.
for _name,(_description,_schema) in list(CAT.items()):
    if 'observation_id' in _schema.get('properties',{}):
        _schema['properties'].pop('observation_id')
        _schema['required'].remove('observation_id')
        CAT[_name]=(_description+' 观测身份由框架绑定最近一次主动采样；未采样或过期会拒绝。',_schema)
