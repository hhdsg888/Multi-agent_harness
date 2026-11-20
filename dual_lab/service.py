import argparse,json,traceback
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from pathlib import Path
from .core import Plant,Fault

def serve(root,port):
    auth=json.loads((root/'auth.local.json').read_text());plant=Plant(root/'state/plant.sqlite3')
    class Handler(BaseHTTPRequestHandler):
        def log_message(self,*a):pass
        def do_POST(self):
            result={};status=200
            try:
                if self.path!='/rpc':raise Fault('NOT_FOUND','unknown path')
                token=self.headers.get('Authorization','').removeprefix('Bearer ')
                actor=next((n for n,t in auth.items() if t==token),None)
                if not actor:raise Fault('FORBIDDEN','authentication required')
                size=int(self.headers.get('Content-Length',0))
                if not 0<size<=524288:raise Fault('BAD_REQUEST','body size outside limit')
                body=json.loads(self.rfile.read(size));result=plant.rpc(actor,body)
            except Fault as e:status=403 if e.code=='FORBIDDEN' else 409;result={'ok':False,'error_code':e.code,'error':str(e)}
            except Exception as e:
                traceback.print_exc();status=500;result={'ok':False,'error_code':'INTERNAL_ERROR','error':str(e)}
            raw=json.dumps(result,ensure_ascii=False).encode();self.send_response(status);self.send_header('Content-Type','application/json');self.send_header('Content-Length',str(len(raw)));self.end_headers()
            try:self.wfile.write(raw)
            except (BrokenPipeError,ConnectionResetError):pass
    server=ThreadingHTTPServer(('127.0.0.1',port),Handler)
    print(json.dumps({'service':'dual-plant','port':port}),flush=True);server.serve_forever()

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--root',type=Path,default=Path(__file__).resolve().parents[1]);p.add_argument('--port',type=int,default=18933);a=p.parse_args();serve(a.root,a.port)
