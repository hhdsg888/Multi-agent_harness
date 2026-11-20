import re
from .common import ROOT, read_json


class KnowledgeBase:
    def __init__(self, path=None):
        self.book = read_json(path or ROOT / "knowledge" / "records.json")

    def search(self, query, limit=3):
        if not isinstance(query, str) or not query.strip():
            raise ValueError("检索词不能为空")
        terms = set(re.findall(r"[a-z_][a-z_0-9]*|[\u4e00-\u9fff]{1,2}", query.lower()))
        ranked = []
        for row in self.book["records"]:
            text = (row["title"] + " " + row["content"] + " " + " ".join(row["tags"])).lower()
            score = sum(term in text for term in terms) + 2 * sum(tag.lower() in query.lower() for tag in row["tags"])
            if score:
                ranked.append((score, row))
        ranked.sort(key=lambda pair: (-pair[0], pair[1]["id"]))
        return {"version": self.book["version"], "scope": self.book["scope"], "query": query,
                "hits": [{"score": score, **row} for score, row in ranked[:max(1, min(6, int(limit)))]]}
