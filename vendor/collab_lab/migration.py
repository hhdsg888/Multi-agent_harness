"""Explicit migration from registered byte-exact predecessors, never arbitrary upgrades."""
import copy
import json
from .common import ROOT,now
from .runtime import RuntimeFault,signature
from .resilience import RuntimeJournal,RunLease

SCHEMA=2

def migrate(directory,current_hashes,reason):
    if not isinstance(reason,str) or not reason.strip():raise ValueError('迁移必须说明原因')
    lease=RunLease(directory)
    try:
        journal=RuntimeJournal(directory); p=journal.load()
        if p['closed']:raise RuntimeFault('SESSION_CLOSED','已结束会话不能迁移')
        known=json.loads((ROOT/'checkpoint_compatibility.json').read_text(encoding='utf-8'))
        if p.get('checkpoint_schema',1)!=1 or p['runtime_hashes'] not in known['predecessors']:
            raise RuntimeFault('MIGRATION_UNSUPPORTED','只允许已注册原版契约迁移，不能跳过未知代码变化')
        if journal.uncertain_requests():raise RuntimeFault('MIGRATION_UNCERTAIN_WRITE','存在未确认请求，拒绝迁移')
        if any(j['state'] in ('queued','running','retry') for j in journal.jobs()):
            raise RuntimeFault('MIGRATION_PENDING_JOB','存在未完成云端作业，保留旧版本续接')
        backup=directory/'checkpoint_before_schema2.json'
        if backup.exists():raise RuntimeFault('MIGRATION_BACKUP_EXISTS','保留现有备份，拒绝覆盖')
        backup.write_text(json.dumps({'payload':p,'sha':signature(p)},ensure_ascii=False),encoding='utf-8')
        updated=copy.deepcopy(p); updated.update(checkpoint_schema=SCHEMA,runtime_hashes=current_hashes)
        updated['migration_history']=[{'from_schema':1,'to_schema':SCHEMA,'at':now(),'reason':reason.strip(),
            'original_sha256':signature(p),'from_runtime_hashes':p['runtime_hashes']}]
        journal.save(updated)
        return {'ok':True,'schema':SCHEMA,'inference_started':False,'backup':backup.name}
    finally:lease.close()
