import argparse
import json
import os
from .common import ROOT, write_json
from .config import load_config
from .providers import Providers
from .server import Application, make_server
from .toolchain import forge_tools


def main():
    parser = argparse.ArgumentParser(description="1.1.2 大小模型协同决策系统")
    sub = parser.add_subparsers(dest="command", required=True)
    serve = sub.add_parser("serve", help="启动交互式系统；不自动调用模型")
    serve.add_argument("--port", type=int)
    sub.add_parser("check-models", help="检查模型连接和可用模型")
    forge = sub.add_parser("forge-tools", help="由Qwen制作、验证和发布基础业务工具包")
    forge.add_argument("--turn-budget", type=int, default=14)
    arguments = parser.parse_args()
    config = load_config()
    if arguments.command == "check-models":
        print(json.dumps(Providers(config).health(), ensure_ascii=False, indent=2))
    elif arguments.command == "forge-tools":
        config["cloud_output_tokens"] = 12000
        print(json.dumps(forge_tools(config, Providers(config), arguments.turn_budget), ensure_ascii=False, indent=2))
    else:
        app = Application(config)
        server = make_server(app, port=arguments.port)
        write_json(ROOT / "server.pid", {"pid": os.getpid(), "port": server.server_address[1]})
        print(f"系统已启动：http://{server.server_address[0]}:{server.server_address[1]}/（空闲，不自动调用模型）", flush=True)
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass
        finally:
            if app.session:
                app.session.close()
            server.server_close()


if __name__ == "__main__":
    main()
