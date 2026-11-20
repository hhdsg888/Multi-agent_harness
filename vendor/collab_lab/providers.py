"""Model transports. Credentials are read at request time and never recorded."""
import copy
import json
import ssl
import threading
import time
import urllib.request
import urllib.error
from pathlib import Path
from .common import dumps, write_json, digest
from .contracts import ARGUMENTS, PLUGIN
from .runtime import RuntimeFault


def decision_schema(actions, schemas=None):
    schema = {"type": "object", "properties": {
        "action": {"type": "string", "enum": list(actions)},
        "arguments": {"type": "object"}, "reason": {"type": "string"}},
        "required": ["action", "arguments"], "additionalProperties": False}
    schema["oneOf"] = [{"properties": {"action": {"enum": [name]}, "arguments": (schemas or {}).get(name,ARGUMENTS.get(name, PLUGIN))}} for name in actions]
    return schema


class IncompleteResponseError(ValueError):
    def __init__(self, message, finish_reason, call_id):
        super().__init__(message)
        self.finish_reason, self.call_id = finish_reason, call_id


class Providers:
    def __init__(self, config):
        self.config = config
        self.counter = 0
        self.lock = threading.Lock()
        self.role_locks = {"edge": threading.Lock(), "cloud": threading.Lock()}

    def _connection(self, role):
        if role == "edge" and self.config.get("edge_backend") == "qwen":
            role = "cloud"
        connection = self.config["models"][role]
        handlers = [urllib.request.ProxyHandler({})]
        headers = {"Content-Type": "application/json"}
        if role == "cloud":
            private = Path(connection.get("private_directory", "."))
            ca = connection.get("ca_filename")
            if ca:
                handlers.append(urllib.request.HTTPSHandler(context=ssl.create_default_context(cafile=str(private / ca))))
            if connection.get("key_filename"):
                headers["Authorization"] = "Bearer " + (private / connection["key_filename"]).read_text(encoding="utf-8").strip()
            elif connection.get("api_key"):
                headers["Authorization"] = "Bearer " + connection["api_key"]
        return connection, urllib.request.build_opener(*handlers), headers

    def health(self, role=None):
        result = {}
        for role in ((role,) if role else ("edge", "cloud")):
            try:
                connection, opener, headers = self._connection(role)
                path = "/api/tags" if role == "edge" and self.config.get("edge_backend") != "qwen" else "/models"
                with opener.open(urllib.request.Request(connection["base_url"].rstrip("/") + path, headers=headers), timeout=8) as response:
                    value = json.load(response)
                models = [row.get("name", row.get("id")) for row in value.get("models", value.get("data", []))]
                result[role] = {"ok": connection["model"] in models, "model": connection["model"]}
            except Exception as error:
                result[role] = {"ok": False, "model": self.config["models"][role].get("model"),
                                "error": f"{type(error).__name__}: {error}"}
        return result

    def call(self, role, messages, actions, evidence):
        # All retries precede returning a validated action. Never replay an executor.
        with self.role_locks[role]:
            is_ollama = role == "edge" and self.config.get("edge_backend") != "qwen"
            budget = self.config['edge_output_tokens'] if is_ollama else None
            transport_failures, response_failures, length_retried = 0,0,False
            retry_of = None
            attempt = 0
            while True:
                attempt += 1
                try:
                    return self._call_once(role, messages, actions, evidence, budget, attempt, retry_of)
                except IncompleteResponseError as error:
                    retry_of = error.call_id
                    ceiling = self.config.get('edge_output_tokens_max',budget)
                    if error.finish_reason=='length' and is_ollama and self.config.get('edge_length_retry') and not length_retried and ceiling>budget:
                        old, budget, length_retried = budget,ceiling,True
                        evidence.add('model_length_retry',role,{'previous_call_id':retry_of,'previous_output_limit':old,
                            'next_output_limit':budget,'same_messages_sha256':digest(messages),'tool_execution_performed':False,'extra_prompt_added':False})
                        continue
                    response_failures += 1
                    if response_failures>=self.config.get('response_retry_attempts',2):
                        raise RuntimeFault('MODEL_RESPONSE_INCOMPLETE','模型响应未完整；有限重试已用尽') from error
                    kind = 'incomplete_response'
                except (json.JSONDecodeError,ValueError) as error:
                    response_failures += 1
                    if response_failures>=self.config.get('response_retry_attempts',2):
                        raise RuntimeFault('MODEL_RESPONSE_INVALID','模型未返回完整有效动作；有限重试已用尽') from error
                    kind = 'invalid_response'
                except (urllib.error.URLError,TimeoutError,ConnectionError,OSError) as error:
                    if isinstance(error,urllib.error.HTTPError) and error.code not in (408,429,500,502,503,504):
                        raise RuntimeFault('TRANSPORT_REJECTED','模型服务拒绝请求，HTTP '+str(error.code)) from error
                    transport_failures += 1
                    if transport_failures>=self.config.get('transport_retry_attempts',3):
                        raise RuntimeFault('TRANSPORT_UNAVAILABLE','模型连接失败；有限重试已用尽') from error
                    kind = 'temporary_transport'
                delays = self.config.get('transport_retry_delays',[1,3])
                delay = delays[min(max(transport_failures-1,0),len(delays)-1)] if kind=='temporary_transport' and delays else 0
                evidence.add('model_request_retry',role,{'attempt':attempt,'reason':kind,'delay_seconds':delay,
                    'same_messages_sha256':digest(messages),'extra_prompt_added':False,'tool_execution_performed':False})
                if delay:
                    time.sleep(delay)

    def _call_once(self, role, messages, actions, evidence, output_tokens=None, attempt=1, retry_of=None):
        with self.lock:
            self.counter += 1
            ident = f"{self.counter:06d}"
        connection, opener, headers = self._connection(role)
        is_ollama = role == "edge" and self.config.get("edge_backend") != "qwen"
        schemas=actions if isinstance(actions,dict) and all(isinstance(v,dict) for v in actions.values()) else None
        schema = decision_schema(actions,schemas)
        body = {"model": connection["model"], "messages": copy.deepcopy(messages)}
        if is_ollama:
            body.update(stream=False, think=self.config.get("edge_thinking", True), format=schema,
                        options={"temperature": 0, "num_ctx": self.config["edge_context"],
                                 "num_predict": output_tokens})
            endpoint = "/api/chat"
        else:
            body.update(stream=True, temperature=0, max_tokens=self.config["cloud_output_tokens"],
                        response_format={"type": "json_object"}, chat_template_kwargs={"enable_thinking": False})
            endpoint = "/chat/completions"
        base = evidence.directory / "calls" / role / ident
        write_json(str(base) + "_request.json", body)
        evidence.add("model_request", role, {"call_id": ident, "model": connection["model"], "actions": list(actions), "attempt":attempt, "retry_of":retry_of, "output_limit":output_tokens})
        started = time.monotonic()
        content, raw, finish = "", None, None
        try:
            request = urllib.request.Request(connection["base_url"].rstrip("/") + endpoint,
                                             data=dumps(body).encode("utf-8"), headers=headers)
            timeout = self.config.get("edge_request_timeout_seconds", self.config["request_timeout_seconds"]) if is_ollama else self.config["request_timeout_seconds"]
            with opener.open(request, timeout=timeout) as response:
                if is_ollama:
                    raw = json.load(response)
                    content = raw.get("message", {}).get("content", "")
                    finish = raw.get("done_reason")
                    if not raw.get("done") or finish == "length":
                        raise IncompleteResponseError("Gemma响应未完整结束", finish, ident)
                else:
                    events, finishes, done = [], [], False
                    raw = {'events':events,'finish_reasons':finishes,'done':False}
                    for line in response:
                        text = line.decode("utf-8").strip()
                        if not text.startswith("data:"):
                            continue
                        data = text[5:].strip()
                        if data == "[DONE]":
                            done = True
                            raw['done'] = True
                            continue
                        item = json.loads(data)
                        events.append(item)
                        if item.get("error"):
                            raise ValueError(str(item["error"]))
                        for choice in item.get("choices", []):
                            content += choice.get("delta", {}).get("content") or ""
                            if choice.get("finish_reason"):
                                finishes.append(choice["finish_reason"])
                    raw = {"events": events, "done": done, "finish_reasons": finishes}
                    finish = finishes[-1] if finishes else None
                    if not done or not finishes or "length" in finishes:
                        raise IncompleteResponseError("Qwen响应未完整结束", finish, ident)
            parsed = json.loads(content)
            if not isinstance(parsed, dict) or not isinstance(parsed.get("action"), str) or not isinstance(parsed.get("arguments"), dict):
                raise ValueError("模型动作不符合公开接口")
            if parsed['action'] not in actions:
                raise ValueError('模型返回了未开放动作')
            result = {"parsed": parsed, "content": content, "raw": raw, "model": connection["model"],
                      "call_id": ident, "seconds": time.monotonic() - started, "finish_reason": finish, "attempt":attempt, "retry_of":retry_of,
                      "usage": {"prompt_tokens":raw.get("prompt_eval_count"),"output_tokens":raw.get("eval_count")} if is_ollama else {}}
            write_json(str(base) + "_response.json", result)
            evidence.add("model_response", role, {key: result[key] for key in ("call_id", "model", "seconds", "parsed", "finish_reason", "usage", "attempt", "retry_of")})
            return result
        except Exception as error:
            result = {"call_id": ident, "error": f"{type(error).__name__}: {error}", "content": content,
                      "raw": raw, "seconds": time.monotonic() - started}
            write_json(str(base) + "_error.json", result)
            evidence.add("model_error", role, {k: v for k, v in result.items() if k != "raw"})
            raise
