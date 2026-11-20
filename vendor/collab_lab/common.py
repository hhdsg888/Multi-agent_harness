import hashlib
import json
import threading
import os
import uuid
from collections import deque
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def now():
    return datetime.now().astimezone().isoformat(timespec="milliseconds")


def dumps(value):
    return json.dumps(value, ensure_ascii=False, allow_nan=False)


def read_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8-sig"))


def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary=path.with_name(path.name+'.tmp.'+uuid.uuid4().hex)
    with temporary.open('w',encoding='utf-8') as stream:
        json.dump(value,stream,ensure_ascii=False,indent=2,allow_nan=False);stream.flush();os.fsync(stream.fileno())
    os.replace(temporary,path)


def digest(value):
    return hashlib.sha256(dumps(value).encode("utf-8")).hexdigest()


class Evidence:
    def __init__(self, directory, restore=False):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.lock = threading.RLock()
        self.events = deque(maxlen=2000)
        self.sequence=0
        if restore and (self.directory/'events.jsonl').exists():
            path=self.directory/'events.jsonl';offset=0
            with path.open('rb') as stream:
                for line in stream:
                    try:event=json.loads(line)
                    except ValueError:
                        tail=line+stream.read();(self.directory/('events_incomplete_'+uuid.uuid4().hex+'.bin')).write_bytes(tail)
                        break
                    self.events.append(event);self.sequence=max(self.sequence,event['id']);offset=stream.tell()
            if offset<path.stat().st_size:
                with path.open('r+b') as stream:stream.truncate(offset)
            if path.stat().st_size:
                with path.open('rb') as stream:stream.seek(-1,2);last=stream.read(1)
                if last!=b'\n':
                    with path.open('ab') as stream:stream.write(b'\n')

    def add(self, kind, actor, data):
        with self.lock:
            self.sequence+=1
            event = {"id": self.sequence, "time": now(), "kind": kind, "actor": actor, "data": data}
            with (self.directory / "events.jsonl").open("a", encoding="utf-8") as stream:
                stream.write(dumps(event) + "\n")
            self.events.append(event)
            return event

    def recent(self, after=0, limit=150):
        with self.lock:
            return [x for x in self.events if x["id"] > after][-limit:]
