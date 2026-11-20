"""Task lifecycle, typed runtime failures and bounded repetition protection."""
import ast
import copy
import hashlib
import json
import threading
import uuid
from collections import Counter
from .common import now, write_json
from .environment import ContractError


class RuntimeFault(ContractError):
    def __init__(self, code, message, retryable=False):
        super().__init__(message)
        self.code, self.retryable = code, retryable


class RepetitionLimit(RuntimeFault):
    def __init__(self, fingerprint, count):
        super().__init__('REPETITION_LIMIT', '重复内容达到运行保护上限，当前任务停止')
        self.fingerprint, self.count = fingerprint, count


def error_packet(error):
    if isinstance(error, RuntimeFault):
        code, retryable = error.code, error.retryable
    elif isinstance(error, ContractError):
        code, retryable = 'TOOL_CONTRACT_ERROR', False
    elif isinstance(error, (KeyError, ValueError, TypeError)):
        code, retryable = 'INVALID_ARGUMENTS', False
    else:
        code, retryable = 'INTERNAL_ERROR', False
    return {'ok': False, 'error_code': code, 'retryable': retryable,
            'error': f'{type(error).__name__}: {error}'}


def explicit_versions(value):
    """Only normalize framework results, never reports, source or model decisions."""
    if isinstance(value, dict):
        opaque = {'report_original','report','editable_report','field_guidance','strategy','arguments','model_submission','implementation','input_schema','readings','body'}
        return {({'version':'config_version', 'previous_version':'previous_config_version',
                  'source_version':'source_config_version'}.get(k, k)): (v if k in opaque else explicit_versions(v))
                for k, v in value.items()}
    if isinstance(value, list):
        return [explicit_versions(v) for v in value]
    return value


TERMINAL = {'solved', 'unresolved', 'blocked_resources', 'failed_repetition', 'failed_interface', 'failed_runtime', 'cancelled'}


class TaskLedger:
    def __init__(self, directory, evidence):
        self.path, self.evidence = directory/'tasks.json', evidence
        self.lock = threading.RLock()
        self.rows, self.active_id = [], None

    def enqueue(self, goal):
        with self.lock:
            row = {'task_id':'task_'+uuid.uuid4().hex[:12], 'task_revision':1,
                   'goal':copy.deepcopy(goal), 'state':'queued', 'created_at':now(),
                   'state_revision':0, 'result':None}
            self.rows.append(row)
            self.save()
            return copy.deepcopy(row)

    def active(self):
        return next((r for r in self.rows if r['task_id']==self.active_id), None)

    def activate_next(self):
        with self.lock:
            if self.active() and self.active()['state'] not in TERMINAL:
                return self.active()
            row = next((r for r in self.rows if r['state']=='queued'), None)
            self.active_id = row['task_id'] if row else None
            if row:
                row['activation_sequence'] = row.get('activation_sequence',0)+1
                self.transition('investigating')
            self.save()
            return row

    def transition(self, state, result=None):
        with self.lock:
            row = self.active()
            if row is None:
                return
            allowed = {'queued','investigating','acting','verifying','waiting_cloud',*TERMINAL}
            if state not in allowed:
                raise ValueError('未知任务状态')
            if row['state'] in TERMINAL and state not in TERMINAL:
                raise RuntimeFault('TASK_TERMINAL', '终态任务须经显式恢复入口重新调查')
            transitions = {'queued':{'investigating'},'investigating':{'acting','verifying','waiting_cloud'},
                           'acting':{'investigating','verifying','waiting_cloud'},
                           'verifying':{'investigating','acting','waiting_cloud'},'waiting_cloud':{'investigating'}}
            if state not in TERMINAL and state!=row['state'] and state not in transitions.get(row['state'],set()):
                raise RuntimeFault('INVALID_TASK_TRANSITION','当前阶段不能直接进入'+state)
            if row['state']==state and result is None:
                return
            previous = row['state']
            row.update(state=state, state_revision=row['state_revision']+1, updated_at=now())
            if result is not None:
                row['result'] = copy.deepcopy(result)
            self.evidence.add('task_transition','runtime',{'task_id':row['task_id'],
                'task_revision':row['task_revision'],'from':previous,'to':state,
                'state_revision':row['state_revision']})
            self.save()

    def identity(self):
        row = self.active()
        return {k:row[k] for k in ('task_id','task_revision')} if row else {}

    def view(self):
        with self.lock:
            return {'active_task_id':self.active_id, 'tasks':copy.deepcopy(self.rows)}

    def save(self):
        write_json(self.path, self.view())


def signature(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',',':')).encode()).hexdigest()


def source_signature(code):
    try:
        return signature(ast.dump(ast.parse(code), include_attributes=False))
    except SyntaxError:
        return signature(code)


class RepetitionGuard:
    def __init__(self, limit=20):
        if type(limit) is not int or limit<2:
            raise ValueError('重复上限须为至少2的整数')
        self.limit = limit
        self.last, self.consecutive = None, 0
        self.failures, self.source_failures = Counter(), Counter()
        self.source_attempts = Counter()
        self.content_visits = Counter()

    def normalized(self, arguments):
        # IDs generated by the runtime are not substantive progress.
        return {k:v for k,v in arguments.items() if k not in
                ('observation_id','request_id','candidate_id','receipt_id')}

    def before(self, action, arguments, context):
        key = signature([action,self.normalized(arguments),context])
        self.consecutive = self.consecutive+1 if key==self.last else 1
        self.last = key
        if action in {'read_catalog','get_report_template','retrieve_knowledge','report_gap','request_cloud_plan','read_context','read_device_api','read_candidate','test_candidate'}:
            self.content_visits[key] += 1
        if self.consecutive>=self.limit or self.failures[key]>=self.limit-1 or self.content_visits[key]>=self.limit:
            raise RepetitionLimit(key, self.limit)
        return key

    def outcome(self, key, ok):
        if ok:
            self.failures.pop(key, None)
        else:
            self.failures[key] += 1

    def before_source(self, code, protocol, contract):
        key = signature([source_signature(code),protocol,contract])
        self.source_attempts[key] += 1
        if self.source_failures[key]>=self.limit-1 or self.source_attempts[key]>=self.limit:
            raise RepetitionLimit(key,self.limit)
        return key

    def tested_source(self, code, protocol, contract, passed):
        key = signature([source_signature(code),protocol,contract])
        if passed:
            self.source_failures.pop(key,None)
        else:
            self.source_failures[key] += 1


def resource_feasibility(observation):
    """A necessary physical connectivity bound, not a routing solution."""
    nodes = {n['id']:n['online'] for n in observation['nodes']}
    links = observation['links']
    blocked, insufficient, bounds = [],[],{}
    for ident, flow in observation['flows'].items():
        required = (flow['requested_mbps']*observation['task']['voice_min_satisfaction'] if ident=='voice'
                    else observation['task']['video_min_mbps'] if ident=='video' else 0)
        frontier = {flow['source']}
        visited = set()
        while frontier:
            node = frontier.pop()
            if node in visited or not nodes.get(node,False):
                continue
            visited.add(node)
            for link in links.values():
                # Management switches/protocol gaps are software-remediable.
                if link['source']==node and link['up'] and link['capacity_mbps']>0:
                    frontier.add(link['target'])
        if flow['target'] not in visited and required>0:
            blocked.append(ident)
        best = {flow['source']:sum(l['capacity_mbps'] for l in links.values())+1}
        for _ in nodes:
            changed = False
            for link in links.values():
                if not link['up'] or not nodes.get(link['source']) or not nodes.get(link['target']):
                    continue
                capacity = min(best.get(link['source'],0),link['capacity_mbps'])
                if capacity>best.get(link['target'],0):
                    best[link['target']], changed = capacity,True
            if not changed:
                break
        bounds[ident] = {'best_single_path_capacity_mbps':best.get(flow['target'],0),'required_mbps':required}
        if best.get(flow['target'],0)+1e-9<required:
            insufficient.append(ident)
    from .assurance import shared_capacity
    shared=shared_capacity(observation)
    return {'shared_capacity':shared,'observation_id':observation['observation_id'],
            'necessary_physical_connectivity':not blocked,'flows_without_physical_path':blocked,
            'necessary_resource_conditions_met':not blocked and not insufficient and shared['status']!='infeasible',
            'flows_below_physical_capacity_floor':insufficient,'capacity_bounds':bounds,
            'scope':'necessary physical path/capacity bounds only; switches/protocol/shared allocation require separate decisions'}
