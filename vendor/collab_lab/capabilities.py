"""Explicit contracts and independent oracles for generated tool families."""
import copy
import math
from .contracts import obj,STRING,PLUGIN
from .environment import ContractError,finite_number,NetworkWorld

NETWORK='network-control-v1'
DIAGNOSTIC='network-diagnostic-v1'
PROTOCOLS=('rescue-v2',NETWORK,DIAGNOSTIC)
SERVICE=obj({'flow_id':STRING,'route':{'type':'array','items':STRING,'minItems':1,'maxItems':12},
             'source_limit_mbps':{'type':'number'}},['flow_id'])
DEVICE=obj({'link_id':STRING,'enabled':{'type':'boolean'},'channel':{'type':'integer','minimum':1,'maximum':11}},['link_id','enabled','channel'])
NETWORK_INPUT=obj({'services':{'type':'array','items':SERVICE,'maxItems':12},
                   'devices':{'type':'array','items':DEVICE,'maxItems':12}})


def input_schema(protocol):
    if protocol=='rescue-v2':return copy.deepcopy(PLUGIN['properties']['input'])
    if protocol==NETWORK:return copy.deepcopy(NETWORK_INPUT)
    if protocol==DIAGNOSTIC:return obj({})
    raise ContractError('未支持的工具契约')


def plugin_schema(protocol):
    return obj({'input':input_schema(protocol),'observation_id':STRING,'preview':{'type':'boolean'}},['input','observation_id'])


def api():
    from .candidates import DEVICE_API
    return {'scope':'受控仿真工具契约，不是任意Python或真实设备授权',
        'function':'唯一def build_operations(arguments, device)，无外部访问；新类型支持有步数限制的有限for循环和容器构造。',
        'device':{'fields':['nodes','links','flows','metrics','task','version','environment_revision'],
                  'links':'以链路ID为键，source/target/protocol/up/enabled/channel/capacity_mbps',
                  'flows':'以业务ID为键，source/target/route/requested_mbps/limit_mbps',
                  'metrics':'以业务ID为键，delivered_mbps/demand_satisfaction等实际指标'},
        'families':{
          'rescue-v2':DEVICE_API,
          NETWORK:{'capabilities':['set_route','set_rate_limit','configure_link'],
            'declaration':'write_candidate必须声明非空capabilities子集，只生成声明范围内的操作',
            'input_schema':NETWORK_INPUT,
            'output':'按services顺序：有route则set_route(flow_id,route)，有source_limit_mbps则set_rate_limit(flow_id,limit_mbps)；再按devices顺序生成configure_link(link_id,enabled,channel)。操作须精确对应输入，不自行改物理up/容量。',
            'operations':[{'op':'set_route','flow_id':'输入业务ID','route':['输入链路ID']},
              {'op':'set_rate_limit','flow_id':'输入业务ID','limit_mbps':'输入source_limit_mbps'},
              {'op':'configure_link','link_id':'输入链路ID','enabled':'输入布尔值','channel':'输入整数'}]},
          DIAGNOSTIC:{'capabilities':['assess_service_targets'],'input_schema':obj({}),
            'output':{'voice_target_met':'bool: metrics.voice.demand_satisfaction不为null且>=task.voice_min_satisfaction-1e-9',
                'video_floor_met':'bool: metrics.video.delivered_mbps>=task.video_min_mbps-1e-9',
                'objective_met':'bool: 以上两项均true'},
            'side_effects':'只返回诊断事实，不能产生配置操作或修改现场。'}}}


def declared_capabilities(protocol, values=None):
    allowed={NETWORK:{'set_route','set_rate_limit','configure_link'},DIAGNOSTIC:{'assess_service_targets'},'rescue-v2':{'device_command'}}
    if protocol not in allowed:raise ContractError('工具类型不受支持')
    values=values if values is not None else (list(allowed[protocol]) if protocol!=NETWORK else [])
    if not isinstance(values,list) or not values or any(not isinstance(v,str) for v in values) or len(values)!=len(set(values)) or not set(values)<=allowed[protocol]:
        raise ContractError('capabilities须为该类型支持的非空能力子集')
    return values


def expected_operations(arguments,device,capabilities):
    if not isinstance(arguments,dict) or set(arguments)-{'services','devices'}:raise ContractError('网络配置输入仅包含services/devices')
    operations=[];seen=set()
    for field in ('services','devices'):
        rows=arguments.get(field,[])
        if not isinstance(rows,list) or len(rows)>12:raise ContractError('配置项须为最多12项的数组')
        for row in rows:
            if not isinstance(row,dict):raise ContractError('配置项须为对象')
            if field=='services':
                ident=row.get('flow_id')
                if ident not in device['flows'] or ident in seen or set(row)-{'flow_id','route','source_limit_mbps'}:raise ContractError('未知/重复业务或未授权字段')
                seen.add(ident)
                if not ('route' in row or 'source_limit_mbps' in row):raise ContractError('业务配置没有实际字段')
                if 'route' in row:
                    route=row['route']
                    if not isinstance(route,list) or not 1<=len(route)<=12 or any(not isinstance(i,str) or i not in device['links'] for i in route):raise ContractError('route须为实际链路ID数组')
                    operations.append({'op':'set_route','flow_id':ident,'route':route})
                if 'source_limit_mbps' in row:
                    finite_number(row['source_limit_mbps'],0,100)
                    operations.append({'op':'set_rate_limit','flow_id':ident,'limit_mbps':row['source_limit_mbps']})
            else:
                ident=row.get('link_id')
                if ident not in device['links'] or ('link',ident) in seen or set(row)!={'link_id','enabled','channel'}:raise ContractError('未知/重复设备或未授权字段')
                seen.add(('link',ident))
                if device['links'][ident]['protocol']!='standard-v1':raise ContractError('通用设备配置仅支持standard-v1，其他协议需独立适配')
                if type(row['enabled']) is not bool or type(row['channel']) is not int or not 1<=row['channel']<=11:raise ContractError('设备配置类型/范围错误')
                operations.append({'op':'configure_link',**row})
    if not 1<=len(operations)<=12 or any(op['op'] not in capabilities for op in operations):raise ContractError('操作超出已声明能力或数量限制')
    return operations


def diagnostic_truth(device):
    voice=device['metrics'].get('voice',{}).get('demand_satisfaction')
    video=device['metrics'].get('video',{}).get('delivered_mbps',0)
    checks={'voice_target_met':voice is not None and voice>=device['task']['voice_min_satisfaction']-1e-9,
        'video_floor_met':video>=device['task']['video_min_mbps']-1e-9}
    return {**checks,'objective_met':all(checks.values())}


def trial_verified(protocol,before,after,inputs,result,goal):
    if protocol==DIAGNOSTIC:return before==after and result['diagnosis']==diagnostic_truth({**before,'task':goal})
    if protocol==NETWORK:
        twin=NetworkWorld(task=goal,state=before);twin.apply(result['operations'],before['version'])
        return twin.snapshot()==after
    actual=after['links'].get(inputs.get('link_id'),{})
    return (actual.get('enabled') is inputs.get('enabled') and actual.get('channel')==inputs.get('channel')
        and all(after['links'][k]==v for k,v in before['links'].items() if k!=inputs.get('link_id'))
        and all(after['flows'][k]['route']==v['route'] and after['flows'][k]['limit_mbps']==v['limit_mbps'] for k,v in before['flows'].items())
        and actual.get('up')==before['links'].get(inputs.get('link_id'),{}).get('up')
        and actual.get('capacity_mbps')==before['links'].get(inputs.get('link_id'),{}).get('capacity_mbps'))
