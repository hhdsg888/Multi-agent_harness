import json,urllib.request,urllib.error
class Client:
    def __init__(self,url,token):self.url=url.rstrip('/')+'/rpc';self.token=token;self.opener=urllib.request.build_opener(urllib.request.ProxyHandler({}))
    def call(self,body):
        req=urllib.request.Request(self.url,data=json.dumps(body,ensure_ascii=False).encode(),headers={'Content-Type':'application/json','Authorization':'Bearer '+self.token})
        try:
            with self.opener.open(req,timeout=30) as r:return json.load(r)
        except urllib.error.HTTPError as e:
            if e.code in (403,409):return json.load(e)
            raise
