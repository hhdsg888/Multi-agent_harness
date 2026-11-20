"""Registered appliance1 interface bindings. No fallback to demonstration data."""
import json,re,urllib.request,urllib.error
from urllib.parse import urlsplit,parse_qsl,urlencode,unquote
from .common import ROOT,read_json
from .runtime import RuntimeFault

class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self,*args,**kwargs):return None

class ApplianceAPI:
    def __init__(self,config):
        self.config=config;self.spec=read_json(ROOT/'appliance_api_contract.json')
        self.opener=urllib.request.build_opener(urllib.request.ProxyHandler({}),NoRedirect())
    def route(self,path,method):
        p=urlsplit(path)
        if p.scheme or p.netloc or p.fragment:raise RuntimeFault('API_PATH_INVALID','只接收相对API路径')
        decoded=unquote(p.path)
        if '\\' in decoded or '//' in decoded or any(s in ('.','..') for s in decoded.split('/')):
            raise RuntimeFault('API_PATH_INVALID','拒绝路径变形')
        for row in self.spec['endpoints']:
            pattern=re.escape(row['path']);pattern=re.sub(r'\\\{[^}]+\\\}',r'[A-Za-z0-9_.-]{1,128}',pattern)
            if re.fullmatch(pattern,decoded):
                if method not in row['methods']:raise RuntimeFault('API_METHOD_NOT_ALLOWED','接口不支持该HTTP方法')
                query=dict(parse_qsl(p.query,keep_blank_values=True))
                for key,value in row.get('query_protection',{}).items():
                    if key in query and query[key].lower()!=value:raise RuntimeFault('API_READ_SIDE_EFFECT','只读查询禁止初始化设备配置')
                    query[key]=value
                return row,decoded+('?' + urlencode(query) if query else '')
        raise RuntimeFault('API_NOT_REGISTERED','接口未登记')
    def catalog(self):
        return {'device':self.spec['device'],'interfaces':[{'path':r['path'],'methods':r['methods'],
            'upstream':r['upstream'],'data_source':r['data_source'],'write_enabled':bool(self.config.get('appliance_allow_writes',False))} for r in self.spec['endpoints']],
            'read_only':not self.config.get('appliance_allow_writes',False),'source_policy':'SOP responses remain mock-labelled; not model decisions'}
    def request(self,path,method='GET',body=None):
        method=method.upper();row,target=self.route(path,method)
        if self.config.get('sensor_profile')!='appliance-live':raise RuntimeFault('API_LIVE_PROFILE_REQUIRED','原设备HTTP接口要求显式实时配置，模拟模式不访问真实设备')
        if method!='GET' and not self.config.get('appliance_allow_writes',False):
            raise RuntimeFault('APPLIANCE_READ_ONLY','本轮只读：写接口已登记，但禁止向一体机发送写请求')
        if method=='GET' and body is not None:raise RuntimeFault('API_BODY_INVALID','GET不发送请求体')
        if method!='GET' and not isinstance(body,dict):raise RuntimeFault('API_BODY_INVALID','写接口请求体须为JSON对象')
        key='appliance_sensor_base_url' if row['upstream']=='device' else 'appliance_sop_base_url'
        base=self.config.get(key,'').rstrip('/');url=urlsplit(base)
        if url.scheme not in ('http','https') or not url.hostname or url.username or url.password or url.query or url.fragment or url.path not in ('','/'):
            raise RuntimeFault('API_BASE_INVALID','接口基地址必须是无凭据/无路径的HTTP源')
        raw=None if body is None else json.dumps(body,ensure_ascii=False).encode('utf-8')
        req=urllib.request.Request(base+target,data=raw,method=method,headers={'Content-Type':'application/json','Accept':'application/json'})
        try:response=self.opener.open(req,timeout=self.config.get('appliance_sensor_timeout_seconds',8))
        except urllib.error.HTTPError as e:response=e
        except (OSError,TimeoutError) as e:
            raise RuntimeFault('EXECUTION_OUTCOME_UNKNOWN' if method!='GET' else 'APPLIANCE_UNAVAILABLE','写请求结果未知，禁止重放' if method!='GET' else '上游接口不可达，无模拟回退') from e
        with response:
            data=response.read(2*1024*1024+1)
            if len(data)>2*1024*1024:raise RuntimeFault('API_RESPONSE_TOO_LARGE','上游响应超过2MiB')
            return {'http_status':response.code,'bytes':data,'content_type':response.headers.get('Content-Type','application/json'),
                'upstream':row['upstream'],'data_source':row['data_source'],'path':target,'method':method}
    def call(self,path,method='GET',body=None):
        r=self.request(path,method,body)
        try:value=json.loads(r['bytes'])
        except Exception as e:raise RuntimeFault('API_NOT_JSON','模型工具要求上游返回JSON') from e
        return {k:v for k,v in r.items() if k not in ('bytes','content_type')} | {'body':value,'scope':'upstream_appliance_api','read_only':method.upper()=='GET'}
