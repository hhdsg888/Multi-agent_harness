"""Bounded evidence checks; never select or execute a model's plan."""
import copy
import itertools
from .runtime import signature, RepetitionLimit


def shared_capacity(observation, budget=10000):
    """Exhaust all unsplittable routes at minimum admitted rates for this simulator.

    Physical switches/protocols are assumed repairable. Feasible is a physical
    bound, not a deployable plan. Search exhaustion cannot prove infeasibility.
    """
    links=observation['links']; online={n['id']:n['online'] for n in observation['nodes']}
    choices=[]; demands=[]
    for fid,f in observation['flows'].items():
        need=(f['requested_mbps']*observation['task']['voice_min_satisfaction'] if fid=='voice'
              else observation['task']['video_min_mbps'] if fid=='video' else 0)
        if need<=0: continue
        if need>f['requested_mbps']+1e-9:return {'status':'infeasible','reason':'demand_below_goal','examined':0}
        paths=[]
        def walk(node,visited,path):
            if len(paths)>budget:return
            if not online.get(node):return
            if node==f['target']:paths.append(path);return
            for lid,l in links.items():
                if l['source']==node and l['target'] not in visited and l['up'] and l['capacity_mbps']+1e-9>=need:
                    walk(l['target'],visited|{l['target']},path+[lid])
        walk(f['source'],{f['source']},[])
        if len(paths)>budget:return {'status':'unknown','reason':'search_budget','examined':0}
        if not paths:return {'status':'infeasible','reason':'no_capacity_path','examined':0}
        choices.append(paths);demands.append(need)
    for count,combo in enumerate(itertools.product(*choices),1):
        if count>budget:return {'status':'unknown','reason':'search_budget','examined':budget}
        loads={lid:sum(d for d,p in zip(demands,combo) if lid in p) for lid in links}
        if all(loads[lid]<=l['capacity_mbps']+1e-9 for lid,l in links.items()):
            return {'status':'feasible','reason':'physical_shared_capacity_bound','examined':count}
    return {'status':'infeasible','reason':'exhausted_all_single_path_assignments','examined':count}


def progress_signature(session):
    s=session.world.snapshot()
    if getattr(session,'appliance',False):
        return signature({'task':session.tasks.identity(),'readings':s['readings'],'tools':[(t['name'],t['tool_version']) for t in session.tools.catalog()['tools']]})
    # Ignore sample IDs, time/queue accumulation and successful no-op writes.
    return signature({'task':session.tasks.identity(),'goal':session.world.task,
        'nodes':s['nodes'],'links':s['links'],
        'flows':{k:{f:v[f] for f in ('route','limit_mbps','requested_mbps')} for k,v in s['flows'].items()},
        'metrics':{k:{f:v[f] for f in ('delivered_mbps','demand_satisfaction')} for k,v in s['metrics'].items()},
        'tools':[(t['name'],t['tool_version']) for t in session.tools.catalog()['tools']]})


def record_progress(session):
    guard=session.guard('edge'); current=progress_signature(session)
    guard.progress_decisions=getattr(guard,'progress_decisions',0)+1
    visits=getattr(guard,'progress_visits',{}); visits[current]=visits.get(current,0)+1
    if len(visits)>128:visits.pop(next(iter(visits)))
    guard.progress_visits=visits
    guard.progress_stalls=guard.progress_stalls+1 if getattr(guard,'progress_last',None)==current else 0
    guard.progress_last=current
    limit=session.config.get('no_progress_limit',60)
    if guard.progress_stalls>=limit or visits[current]>=limit:
        raise RepetitionLimit('no_progress:'+current,max(guard.progress_stalls,visits[current]))
    if guard.progress_decisions>=session.config.get('max_task_decisions',300):
        raise RepetitionLimit('task_decision_budget',guard.progress_decisions)

