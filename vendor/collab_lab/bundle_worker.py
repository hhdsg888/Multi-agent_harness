"""Finite tool worker for generated business code; not a general security sandbox."""
import ast
import json
import sys


def execute(value):
    source = value["code"]
    if not isinstance(source, str) or len(source) > 48000:
        raise ValueError("工具源码长度无效")
    tree = ast.parse(source)
    if not tree.body or any(not isinstance(n, ast.FunctionDef) for n in tree.body):
        raise ValueError("模块只允许函数定义，禁止顶层执行与import")
    names = {n.name for n in tree.body}
    allowed = {"int": int, "float": float, "str": str, "bool": bool, "dict": dict, "list": list,
               "set": set, "tuple": tuple, "len": len, "min": min, "max": max, "sum": sum,
               "all": all, "any": any, "range": range, "enumerate": enumerate, "sorted": sorted,
               "zip": zip, "isinstance": isinstance, "ValueError": ValueError, "abs": abs}
    methods = {"get", "items", "keys", "values", "append", "extend", "copy", "lower", "casefold", "split", "join", "strip",
               "add", "remove", "discard", "issubset", "pop"}
    forbidden = (ast.Import, ast.ImportFrom, ast.ClassDef, ast.Global, ast.Nonlocal,
                 ast.AsyncFunctionDef, ast.With, ast.AsyncWith, ast.While, ast.Await)
    local_functions = {n.name for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)}
    for node in ast.walk(tree):
        if isinstance(node, forbidden):
            raise ValueError("不允许外部访问或无界while循环")
        if isinstance(node, ast.FunctionDef) and (node.decorator_list or node.returns or node.args.defaults or node.args.kw_defaults):
            raise ValueError("不允许装饰器、默认参数和返回注解")
        if isinstance(node, ast.Name) and node.id.startswith("__"):
            raise ValueError("不允许内部名称")
        if isinstance(node, ast.Attribute) and node.attr not in methods:
            raise ValueError("不允许该对象方法：" + node.attr)
        if isinstance(node, ast.Call):
            valid = (isinstance(node.func, ast.Name) and node.func.id in set(allowed) | local_functions) or (isinstance(node.func, ast.Attribute) and node.func.attr in methods)
            if not valid:
                raise ValueError("不允许该函数调用：" + ast.unparse(node.func))
    function = value["function"]
    if function not in names:
        raise ValueError("工具函数不存在")
    namespace = {"__builtins__": allowed}
    exec(compile(tree, "<qwen-business-tools>", "exec"), namespace)
    return namespace[function](value.get("arguments", {}), value["context"])


if __name__ == "__main__":
    try:
        print(json.dumps({"ok": True, "value": execute(json.load(sys.stdin))}, ensure_ascii=True, allow_nan=False))
    except Exception as error:
        print(json.dumps({"ok": False, "error": f"{type(error).__name__}: {error}"}, ensure_ascii=True))
