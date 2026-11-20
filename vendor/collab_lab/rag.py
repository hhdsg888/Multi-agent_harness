"""Auditable lexical RAG over explicit knowledge and strategy collections."""
import copy
import math
import re
import shutil
import uuid
from collections import Counter
from pathlib import Path
from .common import read_json, write_json, digest


def tokenize(text):
    result = []
    for word in re.findall(r"[a-z0-9_+.-]+|[\u3400-\u9fff]+", text.lower()):
        if re.fullmatch(r"[\u3400-\u9fff]+", word):
            result.extend(word[i:i+2] for i in range(max(1, len(word)-1)))
        else:
            result.append(word)
    return result


class RagLibrary:
    def __init__(self, pack, snapshot=None, strategy_pack=None, experience=None):
        self.experience=experience
        source = Path(pack).resolve()
        self.manifest = read_json(source / "manifest.json")
        if snapshot is not None:
            snapshot = Path(snapshot)
            snapshot.mkdir(parents=True, exist_ok=False)
        self.chunks, self.documents = [], []
        files = {}
        for filename in self.manifest["files"]:
            if Path(filename).name != filename or not filename.endswith(".json"):
                raise ValueError("资料文件须由manifest显式列出且位于同目录")
            document_path = source / filename
            if filename == "strategies.json" and strategy_pack is not None:
                document_path = Path(strategy_pack).resolve() / filename
            book = read_json(document_path)
            if book["collection"] not in ("knowledge", "strategy"):
                raise ValueError("未知知识集合")
            files[filename] = book
            for doc in book["documents"]:
                if any(d["id"] == doc["id"] for d in self.documents):
                    raise ValueError("文档ID重复")
                self.documents.append(copy.deepcopy(doc))
                parts = [doc["title"], "关键词：" + " ".join(doc.get("tags", []))]
                parts.extend(str(doc[k]) for k in ("applicability", "content", "evidence") if doc.get(k))
                text = "\n".join(parts)
                size = self.manifest["retrieval"]["chunk_chars"]
                overlap = self.manifest["retrieval"]["overlap_chars"]
                if not 100 <= size <= 4000 or not 0 <= overlap < size:
                    raise ValueError("切片参数无效")
                for n, start in enumerate(range(0, len(text), size - overlap)):
                    piece = text[start:start+size]
                    self.chunks.append({"chunk_id":f"{doc['id']}:{n}","document_id":doc["id"],"collection":book["collection"],
                                        "title":doc["title"],"source_file":filename,"source_version":self.manifest["version"],
                                        "char_start":start,"char_end":start+len(piece),"text":piece})
                    if start + size >= len(text):
                        break
            if snapshot is not None:
                shutil.copyfile(document_path, snapshot / filename)
        self.corpus_hash = digest({"manifest":self.manifest,"files":files})
        self.terms = [Counter(tokenize(c["text"])) for c in self.chunks]
        self.lengths = [sum(t.values()) for t in self.terms]
        self.average = sum(self.lengths)/max(1,len(self.lengths))
        self.df = Counter(t for row in self.terms for t in row)
        self.snapshot = snapshot
        if snapshot is not None:
            shutil.copyfile(source / "manifest.json", snapshot / "manifest.json")
            write_json(snapshot / "index.json", {"corpus_sha256":self.corpus_hash,"method":"BM25","chunks":self.chunks,
                       "document_count":len(self.documents),"chunk_count":len(self.chunks)})

    def info(self):
        return {"method":"BM25","version":self.manifest["version"],"scope":self.manifest["scope"],
                "corpus_sha256":self.corpus_hash,"documents":len(self.documents),"chunks":len(self.chunks),
                "collections":["knowledge","strategy"],"top_k":self.manifest["retrieval"]["top_k"]}

    def search(self, query):
        if not isinstance(query,str) or not query.strip() or len(query)>2000:
            raise ValueError("query须为1至2000字符的检索词")
        expanded = query
        for key, values in self.manifest.get("aliases",{}).items():
            if key.lower() in query.lower() or any(v.lower() in query.lower() for v in values):
                expanded += " " + key + " " + " ".join(values)
        chunks=list(self.chunks)
        experiences=self.experience.documents() if self.experience else []
        for doc in experiences:
            import json
            text=json.dumps(doc,ensure_ascii=False)
            size=self.manifest['retrieval']['chunk_chars'];overlap=self.manifest['retrieval']['overlap_chars']
            for n,start in enumerate(range(0,len(text),size-overlap)):
                piece=text[start:start+size]
                chunks.append({'chunk_id':doc['id']+':'+str(n),'document_id':doc['id'],'collection':doc['collection'],
                    'title':doc['title'],'source_file':'verified_experience/'+doc['collection'],
                    'source_version':doc['evidence_sha256'],'text':piece,'experience_id':doc['experience_id'],
                    'verification':doc['verification'],'char_start':start,'char_end':start+len(piece)})
                if start+size>=len(text):break
        indexed=[Counter(tokenize(c['text'])) for c in chunks]
        lengths=[sum(t.values()) for t in indexed];average=sum(lengths)/max(1,len(lengths))
        df=Counter(t for row in indexed for t in row)
        terms = set(tokenize(expanded))
        ranked = []
        for index,row in enumerate(indexed):
            score = 0.
            for term in terms:
                freq = row.get(term,0)
                if freq:
                    idf = math.log(1+(len(chunks)-df[term]+.5)/(df[term]+.5))
                    score += idf * freq*2.2/(freq+1.2*(.25+.75*lengths[index]/average))
            if score>0:
                ranked.append((score,index))
        ranked.sort(key=lambda pair:(-pair[0],chunks[pair[1]]["chunk_id"]))
        hits = [{**chunks[i],"score":round(score,6)} for score,i in ranked[:self.manifest["retrieval"]["top_k"]]]
        result = {"retrieval_id":uuid.uuid4().hex[:12],"query":query,**self.info(),"hits":hits,
                  "interpretation":"检索片段是合成历史参考；适用条件需结合当前观测，不是当前任务答案。"}
        result.update(experience_documents=len(experiences),experience_corpus_sha256=digest(experiences))
        if self.snapshot is not None:
            write_json(self.snapshot / "queries" / (result["retrieval_id"]+".json"),result)
        return result
