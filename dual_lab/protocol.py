"""Structured, opt-in collaboration control. Models retain business decisions."""
import copy,time,uuid
from .core import Fault,require,GOAL,sha
from .feasibility import link_options

class StructuredProtocol:
    def __init__(self,plant,case,node):self.p=plant;self.c=case;self.n=node;self.peer='B' if node=='A' else 'A'
    def fresh_observation(self,ident):
        try:return self.p.obs(self.c,self.n,ident)
        except Fault:return None
    def peer_observation(self):
        snapshots=[m for m in self.p.received(self.c,self.n) if m['kind']=='resource_snapshot']
        require(snapshots,'PEER_RESOURCE_EVIDENCE_REQUIRED','received peer resource snapshot required')
        observed=snapshots[-1]['payload']['observation']
        require(time.time()-observed['sampled_at']<=1800,'PEER_RESOURCE_EXPIRED','peer snapshot expired')
        receipts=[m for m in self.p.received(self.c,self.n) if m['kind']=='execution_receipt']
        require(not any(m['payload']['config_version']>observed['config_version'] for m in receipts),'PEER_RESOURCE_STALE','known peer configuration changed after resource publication')
        return observed
    def pending_requests(self):
        answered={ident for m in self.c['messages'] if m['sender']==self.n and m['kind']=='resource_snapshot' for ident in m['payload'].get('in_reply_to',[])}
        return [m['message_id'] for m in self.p.received(self.c,self.n) if m['kind']=='resource_request' and m['message_id'] not in answered]
    def active(self):return self.c['plans'].get(self.c.get('active_plan_id'))
    def state(self,ident):
        observation=self.fresh_observation(ident);plan=self.active();available=['read_sensor_adapter','retrieve_knowledge','send_peer_message','request_peer_resources','wait_for_collaboration','get_report_template','report_gap']
        resource_available=False
        try:self.peer_observation();resource_available=True
        except Fault:pass
        channel_available=not self.c['partition']
        # Shared protocol records can be synchronized over the working channel;
        # partitioned nodes receive only their previously known local ledger.
        public=None
        if plan and channel_available:
            public=self.p.public_plan(plan)
            if plan['authorized']:
                seen=self.c.setdefault('seen_authorizations',{'A':[],'B':[]})[self.n]
                if plan['plan_id'] not in seen:seen.append(plan['plan_id']);self.p.save()
            available.append('read_group_plan')
        if observation:
            available+=['finish','record_finding','publish_resource_snapshot','calculate_link_options']
            if not plan or set(plan['applied'])=={'A','B'}:available.append('configure_local_rate')
            if channel_available:
                if self.n=='A' and resource_available:
                    if not plan or set(plan['applied'])=={'A','B'}:available.append('offer_group_plan')
                    elif not plan['authorized']:available.append('revise_group_plan')
                if plan and not plan['authorized']:available.append('review_group_plan')
                if plan and self.n not in plan['applied'] and (plan['authorized'] or self.n=='A' and all(plan['accepted'].get(n) is True and n in plan['ready'] for n in ('A','B'))):available.append('execute_group_plan')
        phase='observation_required' if not observation else 'execution_or_verification' if plan and plan['authorized'] else 'review_current_plan' if plan else 'planning' if resource_available and self.n=='A' else 'resource_exchange'
        return {'node_id':self.n,'role':'coordinator' if self.n=='A' else 'resource_owner','phase':phase,'available_tools':available,
                'local_observation_valid':observation is not None,'peer_resource_snapshot_available':resource_available,'pending_resource_request_ids':self.pending_requests(),
                'current_plan_id':plan['plan_id'] if public else None,'current_plan_hash':plan['plan_hash'] if public else None,'current_plan':public,
                'local_ledger':self.p.local_context(self.c,self.n),'channel_available':channel_available}
    def check_local(self,ob,fragment):
        port=ob['ports'][fragment['path']];voice=ob['measurements']['voice_demand_mbps']
        require(port['physical_up'],'PHYSICAL_UNAVAILABLE','selected local port physically unavailable')
        require(voice+fragment['video_rate_mbps']<=port['capacity_mbps']-ob['background_mbps'],'LOCAL_RESOURCE_LIMIT','local capacity must cover voice and requested video together')
    def reservation(self,ob,plan):
        return {'node_id':self.n,'observation_id':ob['observation_id'],'plan_hash':plan['plan_hash'],'expires_at':time.time()+1800}
    def dispatch(self,name,arg):
        if self.c['partition'] and name in ('offer_group_plan','revise_group_plan','review_group_plan','execute_group_plan','read_group_plan','publish_resource_snapshot','request_peer_resources'):
            raise Fault('PEER_UNAVAILABLE','node message channel unavailable')
        state=self.state(arg.get('observation_id'))
        require(name in state['available_tools'] or name=='read_peer_events','ACTION_PRECONDITION','tool unavailable in current protocol phase')
        if name in ('revise_group_plan','review_group_plan','execute_group_plan','read_group_plan'):
            plan=self.active();require(plan and arg.get('plan_id')==plan['plan_id'] and arg.get('expected_plan_hash')==plan['plan_hash'],'STALE_NEGOTIATION_VIEW','current plan changed after the model input; no automatic retargeting')
        if name=='request_peer_resources':return {'ok':True,'result':self.p.peer(self.c,self.n,'resource_request',{'reason':arg.get('reason','')})}
        if name=='publish_resource_snapshot':
            ob=self.p.obs(self.c,self.n,arg['observation_id'])
            return {'ok':True,'result':self.p.peer(self.c,self.n,'resource_snapshot',{'observation':copy.deepcopy(ob),'in_reply_to':self.pending_requests()})}
        if name=='calculate_link_options':
            ob=self.p.obs(self.c,self.n,arg['observation_id']);observations={self.n:ob}
            require(arg['scope'] in ('local','group'),'INVALID_ARGUMENTS','scope must be local or group')
            if arg['scope']=='group':observations[self.peer]=self.peer_observation()
            return {'ok':True,'result':link_options(observations,GOAL)}
        if name in ('offer_group_plan','revise_group_plan'):return self.propose(name,arg)
        if name=='review_group_plan':return self.review(arg)
        if name=='wait_for_collaboration':
            require(arg['awaited'] in ('peer_resources','plan_update','authorization','execution_receipt'),'INVALID_ARGUMENTS','unknown awaited event')
            self.c.setdefault('waiting',{'A':None,'B':None})[self.n]={'after_sequence':arg['after_sequence'],'awaited':arg['awaited'],'plan_id':self.c.get('active_plan_id')}
            self.p.event('explicit_wait_requested',self.n,{'case_id':self.c['id'],**self.c['waiting'][self.n]})
            return {'ok':True,'result':{'wait_registered':True,'awaited':arg['awaited']}}
        if name=='execute_group_plan':return self.p.semantic_tool(self.c,self.n,name,arg)
        return {'ok':True,'result':self.p.tool(self.c,self.n,name,arg)}
    def propose(self,name,arg):
        require(self.n=='A','FORBIDDEN','A coordinates the active plan; B supplies resources and may request or suggest changes')
        ob=self.p.obs(self.c,self.n,arg['observation_id']);other=self.peer_observation();actions=arg['node_actions']
        require(set(actions)=={'A','B'},'INVALID_PLAN','both fragments required')
        for n,v in actions.items():self.p.valid_actions(self.c,n,v)
        require(actions['A']['path']==actions['B']['path'] and actions['A']['channel']==actions['B']['channel'],'INCOMPATIBLE_ENDPOINTS','same path/channel required')
        self.check_local(ob,actions[self.n])
        options=link_options({self.n:ob,self.peer:other},GOAL)['options'];option=next(x for x in options if x['path']==actions['A']['path'])
        require(option['video_rate_range_mbps'] and all(v['video_rate_mbps']<=option['max_video_mbps'] for v in actions.values()),'OBSERVED_GROUP_RESOURCE_LIMIT','proposed values exceed the received resource evidence; no values changed')
        old=self.active()
        if name=='offer_group_plan':require(not old or set(old['applied'])=={'A','B'},'ACTIVE_NEGOTIATION','revise current plan instead of creating another root')
        else:require(old and not old['authorized'],'PLAN_COMMITTED','committed plan cannot be replaced')
        pid='plan_'+uuid.uuid4().hex[:10]
        plan={'plan_id':pid,'plan_revision':len(self.c['plans'])+1,'group_task_id':self.c['id'],'plan_hash':sha(actions),'node_actions':copy.deepcopy(actions),'proposer':self.n,'accepted':{self.n:True},'decision_intents':{self.n:True},'ready':{},'authorized':False,'applied':[]}
        plan['ready'][self.n]=self.reservation(ob,plan)
        if name=='revise_group_plan':old['superseded_by']=pid;plan['replaces_plan_id']=old['plan_id'];self.p.event('plan_superseded',self.n,{'case_id':self.c['id'],'old_plan_id':old['plan_id'],'new_plan_id':pid})
        self.c['plans'][pid]=plan;self.c['active_plan_id']=pid
        self.p.peer(self.c,self.n,'plan_proposed',self.p.public_plan(plan))
        return {'ok':True,'result':{'protocol_state':'prepared_waiting_peer_review','plan':self.p.public_plan(plan)}}
    def review(self,arg):
        ob=self.p.obs(self.c,self.n,arg['observation_id']);plan=self.active()
        require(not plan['authorized'],'PLAN_COMMITTED','committed plan cannot be re-reviewed')
        require(type(arg['accept']) is bool,'INVALID_ARGUMENTS','accept must be boolean')
        plan.setdefault('decision_intents',{})[self.n]=arg['accept'];error=None
        if arg['accept']:
            try:self.check_local(ob,plan['node_actions'][self.n])
            except Fault as failure:error=failure
        accepted=arg['accept'] and error is None;plan['accepted'][self.n]=accepted
        if accepted:plan['ready'][self.n]=self.reservation(ob,plan)
        else:plan['ready'].pop(self.n,None)
        response={'plan_id':plan['plan_id'],'node_id':self.n,'decision_intent':arg['accept'],'accept':accepted,'local_prepared':accepted,'explanation':arg.get('explanation','')}
        if error:response.update(error_code=error.code,error=str(error))
        self.p.peer(self.c,self.n,'plan_response',response)
        if self.c['fixture']=='T05' and len(plan['ready'])==2 and not self.c['stale_injected']:
            self.c['stale_injected']=True;self.c['nodes']['B']['ports']['backup']['capacity_mbps']=3.;self.c['nodes']['B']['environment_revision']+=1
            self.p.event('hidden_prepare_change','observer',{'case_id':self.c['id']})
        return {'ok':error is None,'result':{'review':response,'plan':self.p.public_plan(plan)},**({'error_code':error.code,'error':str(error)} if error else {})}
