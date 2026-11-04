#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""vetta_proxy —— 极简本地反代：OpenAI 兼容客户端 -> vetta.sh 模型代理

链路：
    client ──(OpenAI /v1/chat/completions)──▶ 本服务 ──(翻译+清洗)──▶ api.vetta.sh ──▶ 模型
    client ◀──(OpenAI chat.completion / SSE chunk)── 本服务 ◀──(OpenAI 响应)──────────┘

翻译层做四件事：
  1. 字段白名单：上游对未知字段一律 400，所以只放行它支持的字段；
  2. 模型短名映射：4a -> google/gemini-4-argon，5.5o -> anthropic/claude-opus-5.5；
  3. 思考挡位归一：reasoning_effort / thinking_level / thinking.budget_tokens 统一成
     low|medium|high，再按 [thinking] mode 决定「剥掉」还是「换成对应模型」；
  4. 多密钥：同一 key 先重试，仍不可用才切换下一个，并把坏 key 摘到 [disabled]。

日志分两层：
  * 控制台（cmd）：每请求一行 —— 时间 + 请求的模型 + 状态码 + 耗时；
  * logs/ 目录：proxy-<run>.log 明细、access.jsonl 结构化汇总、bodies/ 请求与响应全文。

启动：python vetta_proxy.py        停止：Ctrl+C
"""

from __future__ import annotations

import argparse
import configparser
import json
import os
import re
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit

BASE_DIR = Path(__file__).resolve().parent
CFG_PATH = BASE_DIR / "config.ini"

# 上游接受并会处理的字段；不在表里的一律剥掉（否则 400 unknown body field）
KEEP_FIELDS = (
    "messages",
    "stream",
    "max_tokens",
    "max_completion_tokens",
    "temperature",          # 上游接受但不转发，模型跑自己的默认值
    "tools",
)

EFFORT_KEYS = ("reasoning_effort", "thinking_level", "thinkingLevel", "effort")

# 上游每个模型公布的 max_output_tokens
MAX_OUTPUT_TOKENS = 32768

POST_ROUTES = (
    "/v1/chat/completions",
    "/chat/completions",
    "/v1/proxy/openai/v1/chat/completions",
)

# 这些状态码说明「这个 key 本身不能用」→ 重试后仍失败就摘除
KEY_DEAD_STATUS = (401, 403)

# bodies 落盘上限（防止一条超长流把磁盘写爆）
BODY_MAX_BYTES = 1024 * 1024

_log_lock = threading.Lock()
_ini_lock = threading.Lock()
_file_lock = threading.Lock()
_run_log: Path | None = None


# --------------------------------------------------------------------------- #
# 日志：控制台只留一行摘要，明细写文件
# --------------------------------------------------------------------------- #
def init_run_log(log_dir: Path) -> Path:
    """每次启动一个 logs/proxy-YYYYmmdd-HHMMSS.log，明细全写这里。"""
    global _run_log
    log_dir.mkdir(parents=True, exist_ok=True)
    _run_log = log_dir / f"proxy-{time.strftime('%Y%m%d-%H%M%S')}.log"
    _run_log.touch()
    return _run_log


def log_line(tag: str, msg: str) -> None:
    """明细日志：只落 logs/proxy-*.log，不占控制台。"""
    line = f"{time.strftime('%Y-%m-%d %H:%M:%S')} {tag:4s} {msg}"
    if _run_log is None:                 # 启动早期日志文件还没开
        with _log_lock:
            print(line, flush=True)
        return
    with _file_lock:
        try:
            with open(_run_log, "a", encoding="utf-8") as fh:
                fh.write(line + "\n")
        except OSError:
            pass


def log_console(msg: str) -> None:
    """控制台：时间 + 一行摘要。"""
    with _log_lock:
        print(f"{time.strftime('%H:%M:%S')}  {msg}", flush=True)


def mask_key(key: str) -> str:
    if len(key) <= 12:
        return key[:4] + "..."
    return f"{key[:6]}...{key[-4:]}"


# --------------------------------------------------------------------------- #
# 配置
# --------------------------------------------------------------------------- #
class Config:
    def __init__(self, path: Path):
        self.path = path
        cp = configparser.ConfigParser(interpolation=None)
        if not cp.read(path, encoding="utf-8"):
            raise SystemExit(f"找不到配置文件：{path}")

        def get(section: str, key: str, fallback: str = "") -> str:
            return (cp.get(section, key, fallback=fallback) or "").strip()

        self.host = get("proxy", "host", "0.0.0.0")
        self.port = cp.getint("proxy", "port", fallback=8327)
        self.api_key = get("proxy", "api_key")
        self.max_body_bytes = cp.getint("proxy", "max_body_mb", fallback=32) * 1024 * 1024

        self.upstream_url = get(
            "upstream", "url", "https://api.vetta.sh/v1/proxy/openai/v1/chat/completions"
        )
        # 密钥池：keys 逗号分隔（也接受换行）；旧写法 key 仍兼容
        raw_keys = get("upstream", "keys") or get("upstream", "key")
        env_key = os.environ.get("VETTA_API_KEY", "").strip()
        parsed = [k.strip() for k in re.split(r"[,\n]", raw_keys) if k.strip()]
        if env_key:
            parsed.insert(0, env_key)
        self.upstream_keys = parsed
        self.retries_per_key = cp.getint("upstream", "retries_per_key", fallback=1)
        self.retry_backoff_ms = cp.getint("upstream", "retry_backoff_ms", fallback=400)
        self.auto_disable = cp.getboolean("upstream", "auto_disable", fallback=True)
        self.send_effort_header = cp.getboolean("upstream", "send_effort_header", fallback=True)
        self.upstream_timeout = cp.getint("upstream", "timeout", fallback=600)

        self.default_model = get("models", "default_model", "google/gemini-4-argon")
        self.map_unknown_to_default = cp.getboolean("models", "map_unknown_to_default", fallback=False)
        raw = get("models", "list_models").replace(",", "\n")
        self.models = [x.strip() for x in raw.splitlines() if x.strip()]
        self.alias: dict[str, str] = {}
        for key, value in cp.items("models"):
            if key.startswith("alias.") and value.strip():
                self.alias[key[len("alias."):].strip()] = value.strip()

        self.thinking_mode = (get("thinking", "mode", "strip") or "strip").lower()
        self.default_effort = (get("thinking", "default_effort") or "").lower() or None
        self.effort_models = {
            "low": get("thinking", "model_low"),
            "medium": get("thinking", "model_medium"),
            "high": get("thinking", "model_high"),
        }

        self.log_dir = (BASE_DIR / get("logging", "dir", "logs")).resolve()
        self.access_jsonl = cp.getboolean("logging", "access_jsonl", fallback=True)
        self.log_bodies = cp.getboolean("logging", "log_bodies", fallback=True)
        self.bodies_dir = (BASE_DIR / get("logging", "bodies_dir", "logs/bodies")).resolve()
        self.verbose = cp.getboolean("logging", "verbose", fallback=False)

        self.keypool = KeyPool(path, self.upstream_keys, self.auto_disable)

    def resolve(self, name: str) -> str:
        if name in self.alias:
            return self.alias[name]
        if "/" in name:                      # 已经是上游 id（带 provider 前缀）
            return name
        if self.map_unknown_to_default and self.default_model:
            return self.default_model
        return name


# --------------------------------------------------------------------------- #
# 配置文件原地改写（只动两处，注释原样保留）
# --------------------------------------------------------------------------- #
def _section_span(lines: list[str], name: str) -> tuple[int | None, int]:
    header = re.compile(r"^\s*\[([^\]]+)\]\s*$")
    start = None
    for i, line in enumerate(lines):
        m = header.match(line)
        if not m:
            continue
        if start is not None:
            return start, i
        if m.group(1).strip().lower() == name.lower():
            start = i
    return start, len(lines)


def update_ini_value(path: Path, section: str, key: str, value: str) -> bool:
    """把 [section] 里 `key = ...` 换成新值；没有就插到该段末尾。"""
    with _ini_lock:
        try:
            text = path.read_text(encoding="utf-8")
        except OSError:
            return False
        lines = text.splitlines(keepends=True)
        start, end = _section_span(lines, section)
        if start is None:
            lines.append(f"\n[{section}]\n{key} = {value}\n")
            path.write_text("".join(lines), encoding="utf-8")
            return True

        pattern = re.compile(rf"^\s*{re.escape(key)}\s*=", re.IGNORECASE)
        for i in range(start + 1, end):
            if pattern.match(lines[i]):
                lines[i] = f"{key} = {value}\n"
                path.write_text("".join(lines), encoding="utf-8")
                return True

        insert_at = end
        while insert_at > start + 1 and not lines[insert_at - 1].strip():
            insert_at -= 1
        lines.insert(insert_at, f"{key} = {value}\n")
        path.write_text("".join(lines), encoding="utf-8")
        return True


def append_disabled_key(path: Path, key: str, reason: str) -> None:
    """把坏 key 记到 [disabled]，供人工确认 / 恢复。"""
    with _ini_lock:
        try:
            text = path.read_text(encoding="utf-8")
        except OSError:
            return
        if "[disabled]" in text and key in text.split("[disabled]", 1)[1]:
            return   # 已经记过了
        entry = f"{key} = {time.strftime('%Y-%m-%d %H:%M:%S')} {reason}\n"
        lines = text.splitlines(keepends=True)
        start, end = _section_span(lines, "disabled")
        if start is None:
            if not text.endswith("\n"):
                lines.append("\n")
            lines.append(
                "\n[disabled]\n"
                "# 自动摘除的密钥：key = 摘除时间 + 原因。充值或换新 key 后，删掉对应行即可恢复。\n"
            )
            lines.append(entry)
        else:
            insert_at = end
            while insert_at > start + 1 and not lines[insert_at - 1].strip():
                insert_at -= 1
            lines.insert(insert_at, entry)
        path.write_text("".join(lines), encoding="utf-8")


# --------------------------------------------------------------------------- #
# 密钥池
# --------------------------------------------------------------------------- #
class KeyPool:
    def __init__(self, path: Path, keys: list[str], auto_disable: bool):
        self.path = path
        self.auto_disable = auto_disable
        self.lock = threading.Lock()
        self.keys = list(dict.fromkeys(keys))          # 去重且保持顺序
        self.current = self.keys[0] if self.keys else None

    def __len__(self) -> int:
        return len(self.keys)

    def ordered(self) -> list[str]:
        """从上次成功的 key 开始轮转，避免每次都从坏 key 试起。"""
        with self.lock:
            keys = list(self.keys)
            if not keys:
                return []
            if self.current in keys:
                idx = keys.index(self.current)
                return keys[idx:] + keys[:idx]
            return keys

    def mark_ok(self, key: str) -> None:
        with self.lock:
            if key in self.keys:
                self.current = key

    def disable(self, key: str, reason: str) -> None:
        with self.lock:
            if key not in self.keys:
                return
            self.keys.remove(key)
            if self.current == key:
                self.current = self.keys[0] if self.keys else None
            remaining = ",".join(self.keys)
            left = len(self.keys)
        if self.auto_disable:
            update_ini_value(self.path, "upstream", "keys", remaining)
            append_disabled_key(self.path, key, reason)
        log_line("KEY", f"{mask_key(key)} disabled -> [disabled]: {reason}(left={left})")
        log_console(f"!! key {mask_key(key)} disabled ({left} left)")


# --------------------------------------------------------------------------- #
# 请求翻译
# --------------------------------------------------------------------------- #
def normalize_effort(value) -> str | None:
    if not isinstance(value, str):
        return None
    v = value.strip().lower()
    if v in ("low", "medium", "high"):
        return v
    if v in ("minimal", "none", "off"):
        return "low"
    if v in ("xhigh", "max", "maximum"):
        return "high"
    return None


def extract_effort(body: dict) -> str | None:
    for key in EFFORT_KEYS:
        effort = normalize_effort(body.get(key))
        if effort:
            return effort
    thinking = body.get("thinking")
    if isinstance(thinking, dict) and thinking.get("type") not in (None, "disabled", "off"):
        budget = thinking.get("budget_tokens")
        if isinstance(budget, (int, float)):
            if budget <= 2048:
                return "low"
            if budget <= 8192:
                return "medium"
            return "high"
        return "medium"
    return None


def build_payload(body: dict, model: str) -> dict:
    """客户端 OAI 请求 -> 上游请求：白名单 + 模型替换。"""
    payload: dict = {"model": model, "messages": body.get("messages") or []}
    for key in KEEP_FIELDS:
        if key == "messages":
            continue
        if key in body:
            payload[key] = body[key]

    if "stream" in payload:
        payload["stream"] = bool(payload["stream"])

    for key in ("max_completion_tokens", "max_tokens"):
        value = payload.get(key)
        if isinstance(value, int) and value > MAX_OUTPUT_TOKENS:
            payload[key] = MAX_OUTPUT_TOKENS

    return payload


def oai_error(message: str, etype: str = "upstream_error", code: str | None = None, param=None) -> dict:
    return {"error": {"message": message, "type": etype, "code": code, "param": param}}


class ClientFault(Exception):
    """客户端请求本身有问题（400 一类），换 key 没有意义，直接回给客户端。"""

    def __init__(self, status: int, body: bytes, ctype: str):
        self.status = status
        self.body = body
        self.ctype = ctype


class AllKeysFailed(Exception):
    def __init__(self, attempts: list[tuple[str, int, str]]):
        self.attempts = attempts
        super().__init__("; ".join(f"{mask_key(k)}:{s}" for k, s, _ in attempts) or "no key available")


def _post_once(payload: dict, key: str, cfg: Config, effort: str | None):
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(cfg.upstream_url, data=data, method="POST")
    req.add_header("Authorization", f"Bearer {key}")
    req.add_header("Content-Type", "application/json")
    req.add_header("User-Agent", "vetta-proxy/1.2")
    if cfg.send_effort_header and effort:
        # 上游文档没这个头，实测不生效；留着只为将来支持时自动接上
        req.add_header("Vetta-Effort", effort)
    return urllib.request.urlopen(req, timeout=cfg.upstream_timeout)


def call_upstream(payload: dict, cfg: Config, effort: str | None, rid: str):
    """按「同 key 重试 -> 换 key」的顺序打上游，成功返回 (response, key)。"""
    keys = cfg.keypool.ordered()
    if not keys:
        raise AllKeysFailed([])

    attempts: list[tuple[str, int, str]] = []
    for key in keys:
        for attempt in range(cfg.retries_per_key + 1):
            try:
                resp = _post_once(payload, key, cfg, effort)
                cfg.keypool.mark_ok(key)
                log_line("KEY", f"{rid} ok {mask_key(key)} (attempt {attempt + 1})")
                return resp, key
            except urllib.error.HTTPError as exc:
                body = exc.read()
                ctype = exc.headers.get("Content-Type", "application/json")
                detail = body[:160].decode("utf-8", "replace")

                if exc.code in KEY_DEAD_STATUS:
                    attempts.append((key, exc.code, detail))
                    log_line("KEY", f"{rid} {mask_key(key)} -> {exc.code} (attempt {attempt + 1}){detail}")
                    if attempt < cfg.retries_per_key:
                        time.sleep(cfg.retry_backoff_ms / 1000)
                        continue                     # 同一 key 再试一次
                    cfg.keypool.disable(key, f"{exc.code} {detail}")
                    break                            # 换下一个 key
                if exc.code == 429 or exc.code >= 500:
                    attempts.append((key, exc.code, detail))
                    log_line("KEY", f"{rid} {mask_key(key)} -> {exc.code} (attempt {attempt + 1}){detail}")
                    if attempt < cfg.retries_per_key:
                        time.sleep(cfg.retry_backoff_ms / 1000)
                        continue
                    break                            # 换 key，但不摘除
                # 其余 4xx：请求本身的问题，原样抛给客户端
                log_line("UP ", f"{rid} {exc.code} client-side fault, no key switch: {detail}")
                raise ClientFault(exc.code, body, ctype) from exc
            except Exception as exc:                 # noqa: BLE001 网络层
                detail = repr(exc)
                attempts.append((key, 0, detail))
                log_line("NET", f"{rid} {mask_key(key)} -> {detail} (attempt {attempt + 1})")
                if attempt < cfg.retries_per_key:
                    time.sleep(cfg.retry_backoff_ms / 1000)
                    continue
                break

    raise AllKeysFailed(attempts)


# --------------------------------------------------------------------------- #
# HTTP 处理
# --------------------------------------------------------------------------- #
class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "vetta-proxy/1.2"
    sys_version = ""

    cfg: Config = None            # type: ignore[assignment]
    access: "AccessLog" = None    # type: ignore[assignment]

    def log_message(self, fmt, *args):
        if self.cfg.verbose:
            log_line("HTTP", fmt % args)

    # ---- 基础 ----
    def _cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")

    def _send_json(self, code: int, obj: dict):
        data = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self._cors()
        self.end_headers()
        try:
            self.wfile.write(data)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def _err(self, code: int, message: str, etype: str = "invalid_request_error", param=None):
        self._send_json(code, oai_error(message, etype, param=param))

    def _authorized(self) -> bool:
        if not self.cfg.api_key:
            return True
        auth = self.headers.get("authorization") or ""
        token = auth[7:].strip() if auth.lower().startswith("bearer ") else auth.strip()
        return token == self.cfg.api_key or (self.headers.get("x-api-key") or "").strip() == self.cfg.api_key

    def _finish(self, model: str, status: int, t0: float, reason: str = "") -> float:
        """控制台收尾行：模型 + 状态码 + 耗时（失败时跟一句短原因）。"""
        elapsed = time.time() - t0
        line = f"{model:<10} {status:<4} {elapsed:6.2f}s"
        if reason:
            line += f"  {reason}"
        log_console(line)
        return elapsed

    # ---- 路由 ----
    def do_OPTIONS(self):
        self.send_response(204)
        self._cors()
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self):
        path = urlsplit(self.path).path
        if path in ("/healthz", "/health"):
            self._send_json(200, {
                "ok": True,
                "upstream": self.cfg.upstream_url,
                "keys": len(self.cfg.keypool),
                "models": len(self.cfg.models),
                "thinking_mode": self.cfg.thinking_mode,
                "default_effort": self.cfg.default_effort,
            })
            return
        if path in ("/v1/models", "/models"):
            self._send_json(200, {
                "object": "list",
                "data": [
                    {
                        "id": name,
                        "object": "model",
                        "owned_by": "vetta",
                        "upstream_id": self.cfg.alias.get(name, name),
                    }
                    for name in self.cfg.models
                ],
            })
            return
        self._err(404, f"no route: {path}", "not_found")

    def do_POST(self):
        rid = uuid.uuid4().hex[:6]
        path = urlsplit(self.path).path
        t0 = time.time()

        if path not in POST_ROUTES:
            self._err(404, f"no route: {path}", "not_found")
            self._finish("-", 404, t0, "no route")
            return
        if not self._authorized():
            self._err(401, "invalid api key", "authentication_error")
            self._finish("-", 401, t0, "local auth")
            return

        try:
            length = int(self.headers.get("Content-Length") or 0)
        except ValueError:
            length = 0
        if length <= 0:
            self._err(400, "empty request body")
            self._finish("-", 400, t0, "empty body")
            return
        if length > self.cfg.max_body_bytes:
            self._err(413, "request body too large", "request_too_large")
            self._finish("-", 413, t0, "body too large")
            return

        raw = self.rfile.read(length)
        try:
            body = json.loads(raw)
        except json.JSONDecodeError as exc:
            self._err(400, f"invalid json: {exc}")
            self._finish("-", 400, t0, "invalid json")
            return
        if not isinstance(body, dict):
            self._err(400, "request body must be a json object")
            self._finish("-", 400, t0, "not an object")
            return

        effort = extract_effort(body) or self.cfg.default_effort

        model_in = str(body.get("model") or self.cfg.default_model)
        model_up = self.cfg.resolve(model_in)
        if self.cfg.thinking_mode == "model" and effort:
            override = self.cfg.effort_models.get(effort) or ""
            if override:
                model_up = self.cfg.resolve(override)

        stream = bool(body.get("stream"))
        payload = build_payload(body, model_up)
        nmsg = len(payload["messages"])

        log_line(
            "REQ",
            f"{rid} {self.client_address[0]} model={model_in}"
            f"{'' if model_in == model_up else ' -> ' + model_up}"
            f" stream={int(stream)} msgs={nmsg} effort={effort or '-'}"
            f" keys={len(self.cfg.keypool)} bytes={len(raw)}",
        )

        try:
            resp, used_key = call_upstream(payload, self.cfg, effort, rid)
        except ClientFault as fault:
            data = _as_oai_error(fault.body, fault.ctype)
            self._relay_raw(fault.status, data, "application/json")
            elapsed = self._finish(model_in, fault.status, t0, "client request")
            log_line("DONE", f"{rid} {fault.status} client-fault elapsed={elapsed:.2f}s body={fault.body[:300]!r}")
            self._record(rid, model_in, model_up, stream, nmsg, effort, fault.status, 0, elapsed, None, "", "")
            self._dump_body(rid, model_in, model_up, effort, stream, payload, data)
            return
        except AllKeysFailed as exc:
            log_line("DONE", f"{rid} 502 all-keys-failed {exc}")
            self._send_json(502, oai_error(f"all upstream keys failed: {exc}", "upstream_error"))
            elapsed = self._finish(model_in, 502, t0, "all keys failed")
            self._record(rid, model_in, model_up, stream, nmsg, effort, 502, 0, elapsed, None, "", "")
            return

        upstream_rid = resp.headers.get("x-request-id") or ""
        if stream:
            self._relay_stream(resp, rid, model_in, model_up, effort, t0, nmsg, used_key, upstream_rid, payload)
        else:
            self._relay_json(resp, rid, model_in, model_up, effort, t0, nmsg, used_key, upstream_rid, payload)

    # ---- 响应回写（统一成 OAI 形状）----
    def _relay_raw(self, code: int, data: bytes, ctype: str):
        self.send_response(code)
        self.send_header("Content-Type", ctype or "application/json")
        self.send_header("Content-Length", str(len(data)))
        self._cors()
        self.end_headers()
        try:
            self.wfile.write(data)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def _relay_json(self, resp, rid, model_in, model_up, effort, t0, nmsg, used_key, upstream_rid, payload):
        data = resp.read()
        ctype = resp.headers.get("Content-Type", "application/json")

        # 上游若回了非 JSON（例如 Cloudflare 502 页面），包成 OAI 错误体
        if "json" not in (ctype or ""):
            wrapped = _as_oai_error(data, ctype)
            self._relay_raw(502, wrapped, "application/json")
            elapsed = self._finish(model_in, 502, t0, "non-json upstream")
            log_line("DONE", f"{rid} 502 non-json upstream ctype={ctype} bytes={len(data)}")
            self._record(rid, model_in, model_up, False, nmsg, effort, 502, len(data), elapsed, None, used_key, upstream_rid)
            self._dump_body(rid, model_in, model_up, effort, False, payload, wrapped)
            return

        self._relay_raw(200, data, ctype)
        elapsed = self._finish(model_in, 200, t0)
        chars = count_chars(data)
        log_line(
            "DONE",
            f"{rid} 200 chars={chars} bytes={len(data)} elapsed={elapsed:.2f}s"
            f" key={mask_key(used_key)} upstream_req={upstream_rid or '-'}",
        )
        self._record(rid, model_in, model_up, False, nmsg, effort, 200, chars, elapsed, None, used_key, upstream_rid)
        self._dump_body(rid, model_in, model_up, effort, False, payload, data)

    def _relay_stream(self, resp, rid, model_in, model_up, effort, t0, nmsg, used_key, upstream_rid, payload):
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-cache, no-transform")
        self.send_header("X-Accel-Buffering", "no")
        self.send_header("Transfer-Encoding", "chunked")
        self._cors()
        self.end_headers()

        total = 0
        ttft = None
        events: list[bytes] = []
        keep = 0
        try:
            while True:
                line = resp.readline()
                if not line:
                    break
                if ttft is None:
                    ttft = time.time() - t0
                total += len(line)
                if keep < BODY_MAX_BYTES:
                    events.append(line)
                    keep += len(line)
                self._write_chunk(line)
        except (BrokenPipeError, ConnectionResetError):
            log_line("ERR", f"{rid} client disconnected mid-stream")
        except Exception as exc:                        # noqa: BLE001
            log_line("ERR", f"{rid} stream relay error: {exc!r}")
        finally:
            try:
                self._write_chunk(b"", final=True)
            except Exception:
                pass

        elapsed = self._finish(model_in, 200, t0)
        log_line(
            "DONE",
            f"{rid} 200 stream chunks_bytes={total} ttft={f'{ttft:.2f}s' if ttft else '-'}"
            f" elapsed={elapsed:.2f}s key={mask_key(used_key)} upstream_req={upstream_rid or '-'}",
        )
        self._record(rid, model_in, model_up, True, nmsg, effort, 200, total, elapsed, ttft, used_key, upstream_rid)
        self._dump_body(rid, model_in, model_up, effort, True, payload, b"".join(events))

    def _write_chunk(self, data: bytes, final: bool = False):
        if final:
            self.wfile.write(b"0\r\n\r\n")
        else:
            self.wfile.write(b"%x\r\n" % len(data) + data + b"\r\n")
        self.wfile.flush()

    # ---- 落盘 ----
    def _record(self, rid, model_in, model_up, stream, nmsg, effort, status,
                chars, elapsed, ttft, used_key, upstream_rid):
        self.access.write({
            "rid": rid,
            "ts": round(time.time(), 3),
            "model": model_in,
            "upstream_model": model_up,
            "stream": stream,
            "msgs": nmsg,
            "effort": effort,
            "status": status,
            "chars": chars,
            "elapsed": round(elapsed, 3),
            "ttft": round(ttft, 3) if ttft else None,
            "key": mask_key(used_key) if used_key else "",
            "upstream_req": upstream_rid,
            "client_ip": self.client_address[0],
        })

    def _dump_body(self, rid, model_in, model_up, effort, stream, payload, response: bytes):
        if not self.cfg.log_bodies:
            return
        try:
            self.cfg.bodies_dir.mkdir(parents=True, exist_ok=True)
            path = self.cfg.bodies_dir / f"{time.strftime('%Y%m%d-%H%M%S')}-{rid}.json"
            blob = {
                "rid": rid,
                "ts": time.strftime("%Y-%m-%d %H:%M:%S"),
                "model": model_in,
                "upstream_model": model_up,
                "effort": effort,
                "stream": stream,
                "request": payload,
                "response": response.decode("utf-8", "replace")[:BODY_MAX_BYTES],
            }
            path.write_text(json.dumps(blob, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception as exc:                        # noqa: BLE001
            log_line("ERR", f"{rid} body dump failed: {exc!r}")


def _as_oai_error(data: bytes, ctype: str) -> bytes:
    """把任意上游错误体包装成 OpenAI 错误形状，客户端 SDK 才能正常解析。"""
    text = data.decode("utf-8", "replace")
    try:
        obj = json.loads(text)
        if isinstance(obj, dict) and "error" in obj:
            return json.dumps(obj, ensure_ascii=False).encode("utf-8")
    except Exception:
        pass
    return json.dumps(
        oai_error(text[:500] or "upstream error", "upstream_error"),
        ensure_ascii=False,
    ).encode("utf-8")


def count_chars(data: bytes) -> int:
    try:
        obj = json.loads(data)
        return len(obj["choices"][0]["message"].get("content") or "")
    except Exception:
        return len(data)


class AccessLog:
    def __init__(self, cfg: Config):
        self.path: Path | None = None
        self.lock = threading.Lock()
        if cfg.access_jsonl:
            cfg.log_dir.mkdir(parents=True, exist_ok=True)
            self.path = cfg.log_dir / "access.jsonl"

    def write(self, record: dict) -> None:
        if self.path is None:
            return
        line = json.dumps(record, ensure_ascii=False)
        with self.lock:
            try:
                with open(self.path, "a", encoding="utf-8") as fh:
                    fh.write(line + "\n")
            except OSError as exc:
                log_line("ERR", f"access.jsonl write failed: {exc}")


# --------------------------------------------------------------------------- #
# 入口
# --------------------------------------------------------------------------- #
def main() -> int:
    parser = argparse.ArgumentParser(description="vetta.sh 模型代理的极简本地反代")
    parser.add_argument("--config", default=str(CFG_PATH), help="配置文件路径")
    parser.add_argument("--host", help="覆盖监听地址")
    parser.add_argument("--port", type=int, help="覆盖监听端口")
    args = parser.parse_args()

    cfg = Config(Path(args.config))
    if args.host:
        cfg.host = args.host
    if args.port:
        cfg.port = args.port

    if len(cfg.keypool) == 0:
        print("config.ini 的 [upstream] keys 为空，且环境变量 VETTA_API_KEY 未设置")
        return 2

    run_log = init_run_log(cfg.log_dir)

    Handler.cfg = cfg
    Handler.access = AccessLog(cfg)

    try:
        server = ThreadingHTTPServer((cfg.host, cfg.port), Handler)
    except OSError as exc:
        log_line("ERR", f"cannot bind {cfg.host}:{cfg.port}: {exc}")
        print(f"cannot bind {cfg.host}:{cfg.port}: {exc}")
        return 1

    print("=" * 72)
    print("  vetta_proxy 已启动")
    print(f"  监听        http://{cfg.host}:{cfg.port}")
    print(f"  客户端填     http://127.0.0.1:{cfg.port}/v1   （API Key 随便填，或填 config.ini 的 api_key）")
    print(f"  上游        {cfg.upstream_url}")
    print(f"  密钥池      {len(cfg.keypool)} 个（每个失败重试 {cfg.retries_per_key} 次后才换 key）")
    print(f"  模型        共 {len(cfg.models)} 个：{', '.join(cfg.models)}")
    print(f"  思考挡位    mode={cfg.thinking_mode}"
          f"{'（剥掉参数）' if cfg.thinking_mode == 'strip' else '（按挡位换模型）'}"
          f" default={cfg.default_effort or '-'}")
    print(f"  明细日志    {run_log}")
    print(f"  请求汇总    {cfg.log_dir / 'access.jsonl'}")
    print(f"  请求/响应体 {'开 -> ' + str(cfg.bodies_dir) if cfg.log_bodies else '关'}")
    print("=" * 72)
    print("控制台只显示：时间  模型  状态码  耗时；详情看上面几个文件")
    print("按 Ctrl+C 停止\n")

    log_line("BOOT", f"listen={cfg.host}:{cfg.port} keys={len(cfg.keypool)} models={len(cfg.models)}"
                     f" thinking={cfg.thinking_mode}/{cfg.default_effort or '-'}")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n已停止")
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
