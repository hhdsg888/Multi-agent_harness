"""Small pure-function runner; not a general-purpose security sandbox."""
import ast
import json
import sys


def run(value):
    source = value["code"]
    if not isinstance(source, str) or len(source) > 16000:
        raise ValueError("候选源码大小无效")
    tree = ast.parse(source)
    if len(tree.body) != 1 or not isinstance(tree.body[0], ast.FunctionDef) or tree.body[0].name != "build_operations":
        raise ValueError("源码只包含def build_operations(arguments, device)函数")
    function = tree.body[0]
    if function.decorator_list or [a.arg for a in function.args.args] != ["arguments", "device"]:
        raise ValueError("函数签名不匹配")
    allowed = {"int": int, "str": str, "float": float, "bool": bool, "len": len,
               "min": min, "max": max, "ValueError": ValueError, "isinstance": isinstance, "dict": dict, "list": list}
    banned = (ast.Import, ast.ImportFrom, ast.Global, ast.Nonlocal, ast.ClassDef, ast.While,
              ast.For, ast.AsyncFor, ast.AsyncFunctionDef, ast.With, ast.AsyncWith, ast.Lambda,
              ast.ListComp, ast.SetComp, ast.DictComp, ast.GeneratorExp)
    for node in ast.walk(tree):
        if isinstance(node, banned):
            raise ValueError("候选仅允许无外部访问的有限计算函数")
        if isinstance(node, ast.Name) and node.id.startswith("__"):
            raise ValueError("不允许内部名称")
        if isinstance(node, ast.Attribute) and node.attr != "get":
            raise ValueError("只允许字典get方法")
        if isinstance(node, ast.Call):
            if not ((isinstance(node.func, ast.Name) and node.func.id in allowed)
                    or (isinstance(node.func, ast.Attribute) and node.func.attr == "get")):
                raise ValueError("不支持的函数调用")
    namespace = {"__builtins__": allowed}
    exec(compile(tree, "<candidate>", "exec"), namespace)
    result = namespace["build_operations"](value["arguments"], value["device"])
    if not isinstance(result, list) or not 1 <= len(result) <= 12:
        raise ValueError("候选须返回操作列表")
    return result


if __name__ == "__main__":
    try:
        print(json.dumps({"ok": True, "operations": run(json.load(sys.stdin))}, ensure_ascii=False))
    except Exception as error:
        print(json.dumps({"ok": False, "error": f"{type(error).__name__}: {error}"}, ensure_ascii=False))
        sys.exit(1)
