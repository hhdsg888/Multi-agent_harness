"""Initialize the authentication configuration required by the node service."""
import json
from pathlib import Path
import secrets
path=Path(__file__).resolve().parent/'auth.local.json'
if path.exists():
    print('Existing authentication configuration retained')
else:
    with path.open('x',encoding='utf-8') as stream:
        json.dump({node:secrets.token_hex(32) for node in ('A','B','admin')},stream,indent=2)
    print('Created local authentication configuration; values not displayed')
