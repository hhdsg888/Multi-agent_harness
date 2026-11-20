"""Structured, opt-in collaboration control. Models retain business decisions."""
import copy,time,uuid
from .core import Fault,require,GOAL,sha
from .feasibility import link_options
from .policy import flexible

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
    def outgoing_requests(self):
        answered={ident for m in self.p.received(self.c,self.n) if m['kind']=='resource_snapshot' for ident in m['payload'].get('in_reply_to',[])}
        return [m['message_id'] for m in self.c['messages'] if m['sender']==self.n and m['kind']=='resource_request' and m['message_id'] not in answered]
    def own_resources_published(self,observation):
        if observation is None:return False
        for message in reversed(self.c['messages']):
            if message['sender']!=self.n or message['kind']!='resource_snapshot':continue
            published=message['payload']['observation']
            # Identical local resource versions can survive another sensor read.
            # Do not inspect whether the peer has secretly fetched this message.
            return time.time()-published['sampled_at']<=1800 and all(published[k]==observation[k] for k in ('config_version','environment_revision','ports','background_mbps'))
        return False
    def wait_options(self,observation,plan,owed,published,outgoing):
        if observation is None or owed or self.c['partition']:return []
        options=[]
        if published and outgoing:options.append('peer_resources')
        if plan is None:
            if self.n=='B' and published:options.append('plan_update')
            return options
        if plan['authorized']:
            if self.n in plan['applied'] and self.peer not in plan['applied']:options.append('execution_receipt')
            return options
        if not published:return options
        if self.n=='A':
            if plan['accepted'].get('B') is None:options.append('plan_update')
        elif plan['accepted'].get('B') is False:options.append('plan_update')
        elif plan['accepted'].get('B') is True:
            try:self.p.check_ready(self.c,'B',plan);options.append('authorization')
            except Fault:pass
        return options
    def active(self):return self.c['plans'].get(self.c.get('active_plan_id'))
    def state(self,ident):
        observation=self.fresh_observation(ident);plan=self.active();available=['read_sensor_adapter','retrieve_knowledge','get_report_template','report_gap']
        owed=self.pending_requests();outgoing=self.outgoing_requests();published=self.own_resources_published(observation)
        wait_options=self.wait_options(observation,plan,owed,published,outgoing)
        if flexible(self.c):
            wait_options=[]
            if not self.c['partition']:
                if outgoing:wait_options.append('peer_resources')
                if not plan and self.n=='B' and published:wait_options.append('plan_update')
                elif plan and not plan['authorized']:
                    if self.n=='A' and plan['accepted'].get('B') is None:wait_options.append('plan_update')
                    elif self.n=='B' and plan['accepted'].get('B') is True:wait_options.append('authorization')
                elif plan and self.n in plan['applied'] and self.peer not in plan['applied']:wait_options.append('execution_receipt')
        peer_observation=None
        try:peer_observation=self.peer_observation()
        except Fault:pass
        resource_available=peer_observation is not None
        channel_available=not self.c['partition']
        if channel_available:
            available.append('send_peer_message')
            if flexible(self.c):available.append('request_peer_resources')
        if flexible(self.c):available.append('read_local_commitments')
        if wait_options:available.append('wait_for_collaboration')
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
            available+=['finish','record_finding','calculate_link_options']
            if channel_available:
                available.append('publish_resource_snapshot')
                if not flexible(self.c) and not owed and not outgoing:available.append('request_peer_resources')
            if flexible(self.c) or not plan or set(plan['applied'])=={'A','B'}:available.append('configure_local_rate')
            if channel_available:
                if self.n=='A' and resource_available:
                    if not plan or set(plan['applied'])=={'A','B'}:available.append('offer_group_plan')
                    elif not plan['authorized']:available.append('revise_group_plan')
                if plan and not plan['authorized']:available.append('review_group_plan')
                if plan and self.n not in plan['applied'] and (plan['authorized'] or self.n=='A' and all(plan['accepted'].get(n) is True and n in plan['ready'] for n in ('A','B'))):available.append('execute_group_plan')
        phase='observation_required' if not observation else 'resource_response_due' if owed else 'execution_or_verification' if plan and plan['authorized'] else 'review_current_plan' if plan else 'planning' if resource_available and self.n=='A' else 'resource_exchange'
        view={'node_id':self.n,'role':'coordinator' if self.n=='A' else 'resource_owner','phase':phase,'available_tools':available,
                'local_observation_valid':observation is not None,'peer_resource_snapshot_available':resource_available,'pending_resource_request_ids':owed,
                'outgoing_resource_request_ids':outgoing,'own_resource_snapshot_current':published,'allowed_wait_targets':wait_options,
                'local_work':{'observation_required':observation is None,'resource_response_owed':bool(owed),'resource_publication_outdated':bool((owed or outgoing or plan) and not published),
                              'current_plan_review_pending':bool(public and not plan['authorized'] and self.n not in plan['accepted']),
                              'common_plan_owner':'A','peer_can_create_common_plan':self.n=='B'},
                'current_plan_id':plan['plan_id'] if public else None,'current_plan_hash':plan['plan_hash'] if public else None,'current_plan':public,
                'local_ledger':self.p.local_context(self.c,self.n),'channel_available':channel_available}
        if flexible(self.c):view['remaining_seconds']=max(0,self.c['created']+1800-time.time())
        if self.c.get('task_dispatch'):
            if (self.c.get('task_revision') in ('r14','r15') and observation and peer_observation
                    and ('offer_group_plan' in available or 'revise_group_plan' in available)):
                view['plan_calculation_sources'] = {self.n: observation['observation_id'],
                                                    self.peer: peer_observation['observation_id']}
            from .tasks import TaskGraph
            prepared=False
            if plan and self.n in plan['ready']:
                try:self.p.check_ready(self.c,self.n,plan);prepared=True
                except Fault:pass
            view['local_preparation_valid']=prepared
            view['task_dispatch']=TaskGraph(self.p,self.c,self.n).sync(view,ident)
        return view
    def check_local(self,ob,fragment):
        port=ob['ports'][fragment['path']];voice=ob['measurements']['voice_demand_mbps']
        require(port['physical_up'],'PHYSICAL_UNAVAILABLE','selected local port physically unavailable')
        require(voice+fragment['video_rate_mbps']<=port['capacity_mbps']-ob['background_mbps'],'LOCAL_RESOURCE_LIMIT','local capacity must cover voice and requested video together')
    def reservation(self,ob,plan):
        return {'node_id':self.n,'observation_id':ob['observation_id'],'plan_hash':plan['plan_hash'],'expires_at':time.time()+1800}
    def dispatch(self,name,arg,task_context_token=None,model_arguments=None):
        if self.c.get('task_dispatch') and name=='read_peer_events':
            return {'ok':True,'result':self.p.tool(self.c,self.n,name,arg)}
        if self.c.get('task_dispatch') and name!='read_peer_events':
            from .tasks import TaskGraph
            from .catalog import task_catalog
            from .validation import validate
            state=self.state(arg.get('observation_id'));graph=TaskGraph(self.p,self.c,self.n)
            if not flexible(self.c):graph.check(task_context_token)
            if flexible(self.c):
                from .flexible import flexible_catalog
                catalog=flexible_catalog(self.n)
            else:catalog=task_catalog()
            require(name in catalog,'UNKNOWN_TOOL','tool outside public node contract')
            error=validate(model_arguments if model_arguments is not None else arg,catalog[name][1])
            if error:return {'ok':False,'error_code':'INVALID_ARGUMENTS','error':error,'public_argument_schema':catalog[name][1]}
            if model_arguments is not None:
                require(all(key in arg and arg[key]==value for key,value in model_arguments.items()),'BUSINESS_ARGUMENT_MISMATCH','effective business fields must equal original model arguments')
            if name=='finish' and arg.get('status')=='solved':
                barrier=state['task_dispatch']['execution_barrier']
                if barrier:
                    require(barrier['confirmed'],'EXECUTION_BARRIER_REQUIRED','both execution receipts required before successful collaborative closure')
                    ob=self.fresh_observation(arg.get('observation_id'))
                    require(ob and ob['sampled_at']>=graph.d['barriers'][barrier['plan_id']]['opened_at'],'POST_BARRIER_OBSERVATION_REQUIRED','actively sample after both execution receipts are confirmed')
        if self.c['partition'] and name in ('offer_group_plan','revise_group_plan','review_group_plan','execute_group_plan','read_group_plan','publish_resource_snapshot','request_peer_resources'):
            raise Fault('PEER_UNAVAILABLE','node message channel unavailable')
        state=self.state(arg.get('observation_id'))
        if name not in state['available_tools'] and name!='read_peer_events' and not (flexible(self.c) and name=='read_local_commitments'):
            return {'ok':False,'error_code':'ACTION_PRECONDITION','error':'tool unavailable in current protocol phase; no action executed',
                    'protocol_preconditions':{key:copy.deepcopy(state[key]) for key in ('phase','available_tools','local_observation_valid','pending_resource_request_ids','outgoing_resource_request_ids','allowed_wait_targets')}}
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
            require(arg['awaited'] in state['allowed_wait_targets'],'WAIT_DEPENDENCY_REQUIRED','wait target must be a real outstanding peer responsibility; local observation and owed resource replies come first')
            self.c.setdefault('waiting',{'A':None,'B':None})[self.n]={'after_sequence':arg['after_sequence'],'awaited':arg['awaited'],'plan_id':self.c.get('active_plan_id')}
            if flexible(self.c):self.c['waiting'][self.n]['deadline']=min(time.time()+180,self.c['created']+1800)
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
