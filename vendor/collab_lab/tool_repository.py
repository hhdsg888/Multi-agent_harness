"""Transactional, project-local storage for declared and tested tool families."""
import copy
import hashlib
import json
import re
import sqlite3
import sys
from contextlib import contextmanager
from pathlib import Path
from .common import ROOT, now, read_json
from .runtime import RuntimeFault, signature
from .candidates import DEVICE_API
from .contracts import PLUGIN
from .capabilities import NETWORK,DIAGNOSTIC,api,plugin_schema

BACKEND = 'fluid-network-v1'
API = 'rescue-v2-operations-v1'


def compatibility_contract(protocol='rescue-v2'):
    if protocol!='rescue-v2':
        if protocol not in (NETWORK,DIAGNOSTIC):raise RuntimeFault('REPOSITORY_INCOMPATIBLE','未知工具能力契约')
        return {'backend':BACKEND,'execution_api':'controlled-capabilities-v1','protocol':protocol,
            'device_contract_sha256':signature(api()['families'][protocol]),'wrapper_sha256':signature(plugin_schema(protocol)),
            'worker_sha256':hashlib.sha256((ROOT/'collab_lab/capability_worker.py').read_bytes()).hexdigest(),
            'python_min':[3,9],'dependencies':{'host_capability_executor':1}}
    return {'backend': BACKEND, 'execution_api': API, 'protocol': 'rescue-v2',
            'device_contract_sha256': signature(DEVICE_API), 'wrapper_sha256': signature(PLUGIN),
            'worker_sha256': hashlib.sha256((ROOT/'collab_lab/plugin_worker.py').read_bytes()).hexdigest(),
            'python_min': [3, 9], 'dependencies': {'host_device_command': 1}}


def compatibility_errors(manifest, snapshot, backend=BACKEND):
    expected = compatibility_contract(manifest.get('protocol','rescue-v2'))
    declared = manifest.get('compatibility', {})
    errors = [key for key in expected if declared.get(key) != expected[key]]
    if backend != declared.get('backend'):
        errors.append('backend_unavailable')
    if tuple(sys.version_info[:2]) < tuple(declared.get('python_min', [999, 0])):
        errors.append('python_version')
    if expected['protocol']=='rescue-v2' and not any(link.get('protocol') == declared.get('protocol') for link in snapshot['links'].values()):
        errors.append('device_protocol_unavailable')
    return sorted(set(errors))


class ToolRepository:
    def __init__(self, directory):
        self.directory = Path(directory).resolve()
        self.directory.mkdir(parents=True, exist_ok=True)
        self.database = self.directory/'repository.sqlite3'
        with self.transaction() as db:
            if db.execute('PRAGMA user_version').fetchone()[0] not in (0,1):
                raise RuntimeFault('REPOSITORY_SCHEMA_INCOMPATIBLE','仓库格式不受当前程序支持')
            db.executescript('''
                CREATE TABLE IF NOT EXISTS meta (id INTEGER PRIMARY KEY, revision INTEGER NOT NULL);
                INSERT OR IGNORE INTO meta VALUES (1,0);
                CREATE TABLE IF NOT EXISTS artifacts (
                    version TEXT PRIMARY KEY, name TEXT NOT NULL, source BLOB NOT NULL,
                    source_sha256 TEXT NOT NULL, manifest TEXT NOT NULL, manifest_sha256 TEXT NOT NULL,
                    tests TEXT NOT NULL, base_version TEXT, revoked INTEGER NOT NULL DEFAULT 0);
                CREATE TABLE IF NOT EXISTS tools (
                    name TEXT PRIMARY KEY, active_version TEXT, enabled INTEGER NOT NULL DEFAULT 1);
                CREATE TABLE IF NOT EXISTS trials (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, version TEXT NOT NULL, session_id TEXT NOT NULL,
                    passed INTEGER NOT NULL, preview INTEGER NOT NULL, details TEXT NOT NULL, created TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS audit (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, action TEXT NOT NULL, name TEXT,
                    version TEXT, details TEXT NOT NULL, created TEXT NOT NULL);
            ''')
            db.execute('PRAGMA user_version=1')

    @contextmanager
    def transaction(self, write=True):
        db = sqlite3.connect(str(self.database), timeout=15, isolation_level=None)
        db.row_factory = sqlite3.Row
        try:
            db.execute('BEGIN IMMEDIATE' if write else 'BEGIN')
            yield db
            if db.in_transaction:
                db.commit()
        except Exception:
            if db.in_transaction:
                db.rollback()
            raise
        finally:
            db.close()

    def _event(self, db, action, name, version, details):
        db.execute('INSERT INTO audit(action,name,version,details,created) VALUES (?,?,?,?,?)',
                   (action, name, version, json.dumps(details, ensure_ascii=False), now()))
        db.execute('UPDATE meta SET revision=revision+1 WHERE id=1')

    def _artifact(self, db, version):
        row = db.execute('SELECT * FROM artifacts WHERE version=?', (version,)).fetchone()
        if row is None:
            raise RuntimeFault('REPOSITORY_VERSION_NOT_FOUND', '仓库中不存在该版本')
        try:
            manifest = json.loads(row['manifest'])
            if not isinstance(manifest,dict):
                raise ValueError('manifest is not an object')
        except (ValueError,TypeError) as error:
            raise RuntimeFault('REPOSITORY_INTEGRITY','仓库元数据不是有效对象') from error
        if (hashlib.sha256(bytes(row['source'])).hexdigest() != row['source_sha256'] or
                signature(manifest) != row['manifest_sha256'] or
                manifest.get('source_sha256') != row['source_sha256'] or
                manifest.get('id') != row['version'] or manifest.get('name') != row['name']):
            raise RuntimeFault('REPOSITORY_INTEGRITY', '仓库制品或元数据哈希不一致，拒绝加载/执行')
        return row, manifest

    def stage(self, store, version, provenance):
        descriptor = store.descriptor(version)
        source = (store.path(version)/'tool.py').read_bytes()
        sha = hashlib.sha256(source).hexdigest()
        tests = read_json(store.path(version)/'test_result.json')
        if not descriptor.get('tested') or descriptor.get('tested_sha256') != sha or not tests.get('passed') or tests.get('source_sha256') != sha:
            raise RuntimeFault('REPOSITORY_UNTESTED', '只有当前源码接口测试通过的候选可入库试用')
        manifest = {**copy.deepcopy(descriptor), 'source_sha256': sha,
                    'compatibility': compatibility_contract(descriptor['protocol']), 'provenance': copy.deepcopy(provenance)}
        with self.transaction() as db:
            existing = db.execute('SELECT version FROM artifacts WHERE version=?', (version,)).fetchone()
            if existing:
                row, old = self._artifact(db, version)
                self._usable(db, old['name'], version)
                if row['source_sha256'] != sha or any(old.get(k)!=manifest.get(k) for k in ('name','description','protocol','input_schema','capabilities')):
                    raise RuntimeFault('REPOSITORY_VERSION_COLLISION', '不可覆盖已存在版本')
                return old
            db.execute('INSERT OR IGNORE INTO tools(name) VALUES (?)', (manifest['name'],))
            tool = db.execute('SELECT * FROM tools WHERE name=?', (manifest['name'],)).fetchone()
            if not tool['enabled']:
                raise RuntimeFault('REPOSITORY_DISABLED','工具已整体禁用；新版本不能通过重新发布绕过禁用')
            db.execute('INSERT INTO artifacts(version,name,source,source_sha256,manifest,manifest_sha256,tests,base_version) VALUES (?,?,?,?,?,?,?,?)',
                (version, manifest['name'], source, sha, json.dumps(manifest, ensure_ascii=False), signature(manifest),
                 json.dumps(tests, ensure_ascii=False), tool['active_version']))
            self._event(db, 'staged', manifest['name'], version, {'requires_live_trial': True, 'provenance': provenance})
        return manifest

    def _usable(self, db, name, version):
        row, manifest = self._artifact(db, version)
        tool = db.execute('SELECT * FROM tools WHERE name=?', (name,)).fetchone()
        if row['name'] != name or not tool or not tool['enabled'] or row['revoked']:
            raise RuntimeFault('REPOSITORY_DISABLED', '工具或指定版本已禁用，当前会话也不能继续调用')
        return manifest

    @contextmanager
    def execution_lease(self, name, version, snapshot, backend=BACKEND):
        # Serialize disable/update against an in-flight local execution. No tool
        # starts after a disable transaction commits; already completed work stays.
        with self.transaction() as db:
            manifest = self._usable(db, name, version)
            errors = compatibility_errors(manifest, snapshot, backend)
            if errors:
                raise RuntimeFault('REPOSITORY_INCOMPATIBLE', '工具不兼容：'+','.join(errors))
            yield manifest

    def visible(self, name, version):
        with self.transaction(False) as db:
            try:
                self._usable(db, name, version)
                return True
            except RuntimeFault as error:
                if error.code == 'REPOSITORY_DISABLED':
                    return False
                raise

    def active(self, snapshot, reserved_names, backend=BACKEND):
        accepted, rejected = [], []
        with self.transaction(False) as db:
            rows = db.execute('SELECT * FROM tools WHERE enabled=1 AND active_version IS NOT NULL ORDER BY name').fetchall()
            for tool in rows:
                try:
                    artifact, manifest = self._artifact(db, tool['active_version'])
                    errors = compatibility_errors(manifest, snapshot, backend)
                    if artifact['revoked']:
                        errors.append('version_disabled')
                    if tool['name'] in reserved_names:
                        errors.append('reserved_tool_name')
                    if errors:
                        rejected.append({'name': tool['name'], 'version': tool['active_version'], 'reasons': errors})
                        continue
                    accepted.append({'descriptor': manifest, 'source': bytes(artifact['source']), 'tests': json.loads(artifact['tests'])})
                except RuntimeFault as error:
                    rejected.append({'name': tool['name'], 'version': tool['active_version'], 'reasons': [error.code]})
        return accepted, rejected

    def record_trial(self, name, version, session_id, passed, preview, details):
        with self.transaction() as db:
            row, manifest = self._artifact(db, version)
            if row['name'] != name:
                raise RuntimeFault('REPOSITORY_VERSION_MISMATCH', '版本不属于该工具')
            db.execute('INSERT INTO trials(version,session_id,passed,preview,details,created) VALUES (?,?,?,?,?,?)',
                       (version, session_id, int(passed), int(preview), json.dumps(details, ensure_ascii=False), now()))
            self._event(db, 'preview_trial' if preview else 'live_trial', name, version, {'passed': passed, **details})
            tool = db.execute('SELECT * FROM tools WHERE name=?', (name,)).fetchone()
            promoted = False
            reason = 'preview_or_failed_trial'
            if passed and not preview:
                if not tool['enabled'] or row['revoked']:
                    reason = 'disabled'
                elif tool['active_version'] == version:
                    reason = 'already_active'
                elif db.execute("SELECT 1 FROM audit WHERE name=? AND version=? AND action IN ('activated_after_trial','activate','rollback') LIMIT 1",(name,version)).fetchone():
                    reason = 'previously_activated_requires_explicit_reactivation'
                elif tool['active_version'] != row['base_version']:
                    reason = 'active_version_changed_since_staging'
                else:
                    db.execute('UPDATE tools SET active_version=? WHERE name=?', (version, name))
                    self._event(db, 'activated_after_trial', name, version, {'previous_version': tool['active_version']})
                    promoted, reason = True, 'verified_live_trial'
            return {'tool_version': version, 'trial_passed': bool(passed), 'preview': bool(preview),
                    'activated_for_future_sessions': promoted, 'reason': reason}

    def manage(self, action, name, version=None, reason='', expected_revision=None):
        if action not in ('disable', 'enable', 'activate', 'rollback', 'disable_version', 'enable_version'):
            raise RuntimeFault('REPOSITORY_ACTION', '未知仓库管理动作')
        if not isinstance(name, str) or not re.fullmatch(r'[a-z][a-z0-9_]{2,47}', name):
            raise RuntimeFault('REPOSITORY_NAME', '工具名无效')
        if not isinstance(reason, str) or not reason.strip():
            raise RuntimeFault('REPOSITORY_REASON_REQUIRED', '请填写管理操作原因')
        with self.transaction() as db:
            revision = db.execute('SELECT revision FROM meta WHERE id=1').fetchone()[0]
            if type(expected_revision) is not int or revision != expected_revision:
                raise RuntimeFault('REPOSITORY_REVISION_CONFLICT', '仓库已变化，请刷新后再次明确操作')
            tool = db.execute('SELECT * FROM tools WHERE name=?', (name,)).fetchone()
            if tool is None:
                raise RuntimeFault('REPOSITORY_TOOL_NOT_FOUND', '仓库中不存在该工具')
            previous = tool['active_version']
            if action in ('activate', 'rollback', 'disable_version', 'enable_version'):
                row, manifest = self._artifact(db, version)
                if row['name'] != name:
                    raise RuntimeFault('REPOSITORY_VERSION_MISMATCH', '版本不属于该工具')
                if action in ('disable_version','enable_version'):
                    db.execute('UPDATE artifacts SET revoked=? WHERE version=?', (int(action=='disable_version'),version))
                else:
                    if row['revoked']:
                        raise RuntimeFault('REPOSITORY_DISABLED', '已禁用版本不能激活')
                    errors = compatibility_errors(manifest, {'links': {'probe': {'protocol':'rescue-v2'}}})
                    if errors:
                        raise RuntimeFault('REPOSITORY_INCOMPATIBLE','目标版本与当前宿主契约不兼容：'+','.join(errors))
                    verified = db.execute('SELECT 1 FROM trials WHERE version=? AND passed=1 AND preview=0 LIMIT 1', (version,)).fetchone()
                    if not verified:
                        raise RuntimeFault('REPOSITORY_TRIAL_REQUIRED', '必须先有工具职责内的真实执行试用成功记录')
                    if action == 'rollback' and not db.execute("SELECT 1 FROM audit WHERE name=? AND version=? AND action IN ('activated_after_trial','activate','rollback') LIMIT 1", (name,version)).fetchone():
                        raise RuntimeFault('REPOSITORY_ROLLBACK_TARGET', '回滚目标必须曾经激活')
                    db.execute('UPDATE tools SET active_version=? WHERE name=?', (version, name))
            else:
                db.execute('UPDATE tools SET enabled=? WHERE name=?', (int(action=='enable'), name))
            self._event(db, action, name, version, {'reason': reason, 'previous_version': previous})
        return self.view()

    def view(self):
        with self.transaction(False) as db:
            tools = [dict(row) for row in db.execute('SELECT * FROM tools ORDER BY name')]
            versions = []
            for row in db.execute('SELECT version,name,source_sha256,manifest,revoked FROM artifacts ORDER BY rowid'):
                manifest = json.loads(row['manifest'])
                versions.append({'version': row['version'], 'name': row['name'], 'source_sha256': row['source_sha256'],
                                 'revoked': bool(row['revoked']), 'compatibility': manifest['compatibility'],
                                 'provenance': manifest['provenance'], 'description': manifest['description']})
            return {'revision': db.execute('SELECT revision FROM meta WHERE id=1').fetchone()[0],
                    'tools': tools, 'versions': versions,
                    'trials': [{**dict(row), 'details': json.loads(row['details'])} for row in db.execute('SELECT * FROM trials ORDER BY id')],
                    'audit': [{**dict(row), 'details': json.loads(row['details'])} for row in db.execute('SELECT * FROM audit ORDER BY id')],
                    'scope': 'project-local declared tool families; interface and actual-use trials do not imply business recovery'}
