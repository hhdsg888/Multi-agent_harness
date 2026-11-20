"""Finite, isolated pure computation for declared tool contracts, not a general sandbox."""
import ast,json,sys


def run(payload):
    code=payload['code']
    if not isinstance(code,str) or len(code)>24000:raise ValueError('候选源码大小无效')
    tree=ast.parse(code)
    if len(tree.body)!=1 or not isinstance(tree.body[0],ast.FunctionDef) or tree.body[0].name!='build_operations':raise ValueError('只允许build_operations函数')
    f=tree.body[0]
    if f.decorator_list or [a.arg for a in f.args.args]!=['arguments','device'] or f.args.defaults or f.args.vararg or f.args.kwarg:raise ValueError('函数签名不匹配')
    allowed={'int':int,'float':float,'str':str,'bool':bool,'len':len,'min':min,'max':max,'ValueError':ValueError,'isinstance':isinstance,'dict':dict,'list':list}
    banned=(ast.Import,ast.ImportFrom,ast.ClassDef,ast.Global,ast.Nonlocal,ast.While,ast.AsyncFor,ast.AsyncFunctionDef,ast.With,ast.AsyncWith,ast.Lambda,ast.ListComp,ast.DictComp,ast.SetComp,ast.GeneratorExp)
    for node in ast.walk(tree):
        if isinstance(node,banned) or isinstance(node,ast.Name) and node.id.startswith('__'):raise ValueError('不允许外部访问或无限循环')
        if isinstance(node,ast.Attribute) and node.attr not in ('get','append','extend','items','values'):raise ValueError('方法未获准')
        if isinstance(node,ast.Call) and not (isinstance(node.func,ast.Name) and node.func.id in allowed or isinstance(node.func,ast.Attribute) and node.func.attr in ('get','append','extend','items','values')):raise ValueError('函数调用未获准')
    count=[0]
    def trace(frame,event,arg):
        if frame.f_code.co_filename=='<candidate>' and event=='line':
            count[0]+=1
            if count[0]>20000:raise ValueError('候选计算步数超限')
        return trace
    namespace={'__builtins__':allowed};sys.settrace(trace)
    try:exec(compile(tree,'<candidate>','exec'),namespace);return namespace['build_operations'](payload['arguments'],payload['device'])
    finally:sys.settrace(None)


if __name__=='__main__':
    try:print(json.dumps({'ok':True,'operations':run(json.load(sys.stdin))},ensure_ascii=False,allow_nan=False))
    except Exception as error:
        print(json.dumps({'ok':False,'error':type(error).__name__+': '+str(error)},ensure_ascii=False));sys.exit(1)
