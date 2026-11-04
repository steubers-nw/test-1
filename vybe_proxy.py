#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
vybe_proxy.py — vybe.build → OpenAI 兼容协议代理（多 Cookie 池版）

基于 steubers-nw/test-1 的 vetta_proxy.py 骨架改造。
仅依赖 Python 标准库。
"""
from __future__ import annotations

import configparser
import hashlib
import json
import os
import sys
import time
import threading
import uuid
import urllib.request
import urllib.error
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any


# =========================================================
# 配置
# =========================================================

class Config:
    def __init__(self, path: str):
        cp = configparser.ConfigParser()
        if not os.path.exists(path):
            raise SystemExit(f"config file not found: {path}")
        cp.read(path, encoding="utf-8")

        if not cp.has_section("upstream"):
            raise SystemExit("config.ini missing [upstream] section")

        self.host = cp.get("server", "host", fallback="127.0.0.1")
        self.port = cp.getint("server", "port", fallback=18186)

        self.upstream_url = cp.get("upstream", "url", fallback="https://vybe.build").rstrip("/")
        self.timeout = cp.getint("upstream", "timeout", fallback=180)

        self.cookie_cooldown      = cp.getint("upstream", "cookie_cooldown", fallback=300)
        self.cookie_max_fails     = cp.getint("upstream", "cookie_max_fails", fallback=3)
        self.cookie_fail_cooldown = cp.getint("upstream", "cookie_fail_cooldown", fallback=60)

        self.cookies: list[CookieEntry] = []
        for section in cp.sections():
            if not section.startswith("cookie:"):
                continue
            name = section[len("cookie:"):].strip()
            if not name:
                raise SystemExit(f"[{section}] empty cookie name")
            token    = cp.get(section, "session_token", fallback="").strip()
            agent_id = cp.get(section, "agent_id", fallback="").strip()
            enabled  = cp.getboolean(section, "enabled", fallback=True)
            if not token or not agent_id:
                raise SystemExit(f"[{section}] missing session_token or agent_id")
            self.cookies.append(CookieEntry(name, token, agent_id, enabled))

        if not self.cookies:
            raise SystemExit("no [cookie:xxx] sections found")

        self.log_to_file = cp.getboolean("log", "to_file", fallback=False)
        self.log_file    = cp.get("log", "file", fallback="proxy.log")


# =========================================================
# 日志
# =========================================================

class Logger:
    def __init__(self, cfg: Config):
        self._lock = threading.Lock()
        self._fp = (open(cfg.log_file, "a", encoding="utf-8", buffering=1)
                    if cfg.log_to_file else None)

    def _write(self, level: str, msg: str):
        line = f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] [{level}] {msg}\n"
        with self._lock:
            sys.stderr.write(line)
            sys.stderr.flush()
            if self._fp:
                self._fp.write(line)

    def info(self, m):  self._write("INFO",  m)
    def warn(self, m):  self._write("WARN",  m)
    def error(self, m): self._write("ERROR", m)


# =========================================================
# Cookie 条目 + 池
# =========================================================

class CookieEntry:
    """一个账户 = 一组 (session_token, agent_id)。"""
    __slots__ = (
        "name", "session_token", "agent_id", "enabled",
        "cooldown_until", "fail_count", "last_error", "_lock",
    )

    def __init__(self, name: str, session_token: str, agent_id: str, enabled: bool):
        self.name = name
        self.session_token = session_token
        self.agent_id = agent_id
        self.enabled = enabled
        self.cooldown_until = 0.0
        self.fail_count = 0
        self.last_error = ""
        self._lock = threading.Lock()

    def available(self) -> bool:
        if not self.enabled:
            return False
        return time.time() >= self.cooldown_until

    def mark_success(self):
        with self._lock:
            self.fail_count = 0
            self.last_error = ""

    def mark_auth_failed(self, msg: str):
        """401：会话失效，永久禁用（需人工恢复）。"""
        with self._lock:
            self.enabled = False
            self.last_error = f"auth: {msg[:200]}"

    def mark_quota_exhausted(self, msg: str, cooldown: int):
        """402/429：额度耗尽 / 限流，冷却一段时间后自动恢复。"""
        with self._lock:
            self.cooldown_until = time.time() + cooldown
            self.last_error = f"quota: {msg[:200]}"

    def mark_transient_fail(self, msg: str, max_fails: int, cooldown: int):
        """5xx / 网络错误：累计失败，达阈值后冷却。"""
        with self._lock:
            self.fail_count += 1
            self.last_error = f"fail: {msg[:200]}"
            if self.fail_count >= max_fails:
                self.cooldown_until = time.time() + cooldown
                self.fail_count = 0

    def snapshot(self) -> dict:
        return {
            "name": self.name,
            "agent_id": self.agent_id,
            "enabled": self.enabled,
            "available": self.available(),
            "cooldown_remaining": max(0, int(self.cooldown_until - time.time())),
            "fail_count": self.fail_count,
            "last_error": self.last_error,
        }


class CookiePool:
    def __init__(self, entries: list[CookieEntry]):
        self._entries = entries
        self._by_name = {e.name: e for e in entries}
        self._rr = 0
        self._lock = threading.Lock()

    def get(self, name: str) -> CookieEntry | None:
        return self._by_name.get(name)

    def available_entries(self, preferred: str | None = None) -> list[CookieEntry]:
        """
        当前可用 cookie 列表，按优先级排序：
          - preferred（若可用）放最前
          - 其余从 round-robin 游标开始轮转
        """
        with self._lock:
            n = len(self._entries)
            start = self._rr
            self._rr = (self._rr + 1) % max(1, n)

        order: list[CookieEntry] = []
        if preferred:
            e = self._by_name.get(preferred)
            if e and e.available():
                order.append(e)
        for i in range(n):
            e = self._entries[(start + i) % n]
            if e in order:
                continue
            if e.available():
                order.append(e)
        return order

    def status(self) -> list[dict]:
        return [e.snapshot() for e in self._entries]

    def any_available(self) -> bool:
        return any(e.available() for e in self._entries)


# =========================================================
# 消息解析
# =========================================================

def _extract_text(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "".join(
            p.get("text", "") for p in content
            if isinstance(p, dict) and p.get("type") == "text"
        )
    return ""


def _collect_from_messages(messages: list) -> tuple[str, str]:
    user_text = ""
    system_parts = []
    for m in messages:
        if not isinstance(m, dict):
            continue
        role = m.get("role")
        if role == "user":
            user_text = _extract_text(m.get("content"))
        elif role == "system":
            t = _extract_text(m.get("content"))
            if t:
                system_parts.append(t)
    return user_text, "\n\n".join(system_parts)


def _thread_key(messages: list) -> str:
    for m in messages:
        if isinstance(m, dict) and m.get("role") == "user":
            return hashlib.sha256(
                _extract_text(m.get("content")).encode("utf-8")
            ).hexdigest()[:16]
    return uuid.uuid4().hex[:16]


def _build_vybe_payload(thread_id: str, user_text: str, system_text: str) -> dict:
    """构造 vybe UIMessage 请求体（有状态，只发当前消息）。"""
    text = user_text
    if system_text:
        text = f"[System instructions]\n{system_text}\n\n[User]\n{user_text}"
    return {
        "trigger": "submit-message",
        "id": thread_id,
        "message": {
            "id": str(uuid.uuid4()),
            "parts": [{"type": "text", "text": text}],
            "metadata": {"createdAt": int(time.time() * 1000)},
            "role": "user",
        },
    }


# =========================================================
# SSE 解析
# =========================================================

def _parse_sse_line(line: str) -> dict | None:
    if not line.startswith("data:"):
        return None
    raw = line[5:].strip()
    if not raw:
        return None
    if raw == "[DONE]":
        return {"type": "__done__"}
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return None


# =========================================================
# 上游异常
# =========================================================

class UpstreamAuthError(Exception): pass
class UpstreamQuotaError(Exception): pass


class UpstreamGenericError(Exception):
    def __init__(self, status: int, body: str):
        super().__init__(f"upstream {status}: {body[:200]}")
        self.status = status
        self.body = body


# =========================================================
# 上游通信
# =========================================================

class Upstream:
    def __init__(self, cfg: Config, logger: Logger):
        self.cfg = cfg
        self.logger = logger

    def open_stream(self, cookie: CookieEntry, thread_id: str, payload: dict):
        """对单个 cookie 发起一次 POST /chat。成功返回 response，失败抛分类异常。"""
        url = f"{self.cfg.upstream_url}/api/agents/{cookie.agent_id}/threads/{thread_id}/chat"
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        req = urllib.request.Request(url, data=data, method="POST")
        req.add_header(
            "Cookie",
            f"__Secure-better-auth.session_token={cookie.session_token}"
        )
        req.add_header("Content-Type", "application/json")
        req.add_header("Accept", "text/event-stream")
        try:
            return urllib.request.urlopen(req, timeout=self.cfg.timeout)
        except urllib.error.HTTPError as e:
            try:
                body = e.read().decode("utf-8", errors="replace")
            except Exception:
                body = ""
            if e.code == 401:
                raise UpstreamAuthError(f"session expired: {body[:200]}") from e
            if e.code in (402, 429):
                raise UpstreamQuotaError(f"quota/rate: {body[:200]}") from e
            raise UpstreamGenericError(e.code, body) from e
        except urllib.error.URLError as e:
            raise UpstreamGenericError(502, f"network: {e.reason}") from e


# =========================================================
# Handler
# =========================================================

class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "vybe-proxy/2.0"

    cfg: Config = None          # type: ignore
    logger: Logger = None       # type: ignore
    upstream: Upstream = None   # type: ignore
    pool: CookiePool = None     # type: ignore

    # thread_key -> (cookie_name, thread_id)
    _threads: dict[str, tuple[str, str]] = {}
    _threads_lock = threading.Lock()
    _thread_max = 1024

    # -------- 基础工具 --------

    def _set_cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Authorization, Content-Type")
        self.send_header("Access-Control-Max-Age", "86400")

    def _send_json(self, status: int, obj: dict):
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self._set_cors()
        self.end_headers()
        self.wfile.write(body)

    def _err(self, status: int, code: str, message: str):
        self._send_json(status, {
            "error": {"message": message, "type": "proxy_error", "code": code}
        })

    def _read_body(self) -> bytes:
        try:
            length = int(self.headers.get("Content-Length") or 0)
        except ValueError:
            length = 0
        return self.rfile.read(length) if length > 0 else b""

    def _start_chunked(self, content_type: str):
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Cache-Control", "no-cache, no-transform")
        self.send_header("X-Accel-Buffering", "no")
        self.send_header("Transfer-Encoding", "chunked")
        self._set_cors()
        self.end_headers()

    def _chunk(self, data: bytes):
        if not data:
            return
        self.wfile.write(f"{len(data):X}\r\n".encode("ascii"))
        self.wfile.write(data)
        self.wfile.write(b"\r\n")
        self.wfile.flush()

    def _chunk_end(self):
        self.wfile.write(b"0\r\n\r\n")
        self.wfile.flush()

    def log_message(self, fmt, *args):
        self.logger.info(f"{self.address_string()} - {fmt % args}")

    # -------- 路由 --------

    def do_OPTIONS(self):
        self.send_response(204)
        self._set_cors()
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self):
        path = self.path.split("?", 1)[0]
        if path == "/healthz":
            with self._threads_lock:
                n = len(self._threads)
            self._send_json(200, {
                "ok": True,
                "upstream": self.cfg.upstream_url,
                "threads": n,
                "cookies": self.pool.status(),
            })
            return
        if path == "/v1/models":
            self._send_json(200, {
                "object": "list",
                "data": [{
                    "id": "vybe", "object": "model",
                    "created": int(time.time()), "owned_by": "vybe",
                }],
            })
            return
        self._err(404, "not_found", f"no route for GET {path}")

    def do_POST(self):
        path = self.path.split("?", 1)[0]
        if path == "/v1/chat/completions":
            try:
                self._handle_chat()
            except Exception as e:
                self.logger.error(f"unhandled in /v1/chat/completions: {e!r}")
                try:
                    self._err(500, "internal_error", str(e))
                except Exception:
                    pass
            return
        self._err(404, "not_found", f"no route for POST {path}")

    # -------- thread 绑定 --------

    def _resolve_thread(self, thread_key: str, cookie_name: str) -> str:
        """
        返回该对话在此 cookie 下的 thread_id。
        绑定存在且 cookie 一致 → 复用；否则新建并更新绑定。
        切换 cookie 会自动换 thread（上下文重置）。
        """
        with self._threads_lock:
            binding = self._threads.get(thread_key)
            if binding and binding[0] == cookie_name:
                return binding[1]
            if len(self._threads) >= self._thread_max:
                try:
                    self._threads.pop(next(iter(self._threads)))
                except StopIteration:
                    pass
            tid = str(uuid.uuid4())
            self._threads[thread_key] = (cookie_name, tid)
            return tid

    def _bindings_snapshot(self, thread_key: str) -> tuple[str, str] | None:
        with self._threads_lock:
            return self._threads.get(thread_key)

    # -------- 核心 --------

    def _handle_chat(self):
        raw = self._read_body()
        if not raw:
            self._err(400, "invalid_request", "empty request body"); return
        try:
            body = json.loads(raw)
        except json.JSONDecodeError as e:
            self._err(400, "invalid_json", str(e)); return

        messages = body.get("messages") or []
        if not isinstance(messages, list) or not messages:
            self._err(400, "invalid_request", "messages must be a non-empty list"); return

        user_text, system_text = _collect_from_messages(messages)
        if not user_text:
            self._err(400, "invalid_request", "no user message found"); return

        stream = bool(body.get("stream", False))
        model_name = body.get("model") or "vybe"
        thread_key = _thread_key(messages)

        # 优先选之前绑定的 cookie（若有）
        binding = self._bindings_snapshot(thread_key)
        preferred = binding[0] if binding else None

        resp = self._open_with_failover(thread_key, preferred, user_text, system_text)
        if resp is None:
            return  # _open_with_failover 已写错误响应

        if stream:
            self._relay_stream(resp, model_name)
        else:
            self._relay_json(resp, model_name)

    def _open_with_failover(
        self, thread_key: str, preferred: str | None,
        user_text: str, system_text: str,
    ):
        """
        按池中的可用列表逐个尝试。成功返回 response；全部失败写错误响应并返回 None。
        """
        candidates = self.pool.available_entries(preferred)
        if not candidates:
            self._err(503, "no_cookie",
                      "no available cookie in pool (all disabled/cooling)")
            return None

        last_error: tuple[str, int, str] | None = None

        for cookie in candidates:
            thread_id = self._resolve_thread(thread_key, cookie.name)
            payload = _build_vybe_payload(thread_id, user_text, system_text)
            try:
                resp = self.upstream.open_stream(cookie, thread_id, payload)
                cookie.mark_success()
                self.logger.info(f"cookie={cookie.name} thread={thread_id[:8]}... ok")
                return resp
            except UpstreamAuthError as e:
                cookie.mark_auth_failed(str(e))
                self.logger.warn(f"cookie={cookie.name} auth failed: {e}")
                last_error = ("auth_error", 401, str(e))
            except UpstreamQuotaError as e:
                cookie.mark_quota_exhausted(str(e), self.cfg.cookie_cooldown)
                self.logger.warn(f"cookie={cookie.name} quota: {e}")
                last_error = ("insufficient_quota", 429, str(e))
            except UpstreamGenericError as e:
                cookie.mark_transient_fail(
                    str(e), self.cfg.cookie_max_fails, self.cfg.cookie_fail_cooldown
                )
                self.logger.warn(f"cookie={cookie.name} fail: {e}")
                last_error = ("upstream_error",
                              502 if e.status >= 500 else e.status, str(e))

        if last_error:
            code, status, msg = last_error
            self._err(status, code, f"all cookies failed; last: {msg}")
        else:
            self._err(503, "no_cookie", "all cookies exhausted")
        return None

    # -------- 流式 --------

    def _relay_stream(self, resp, model_name: str):
        chunk_id = f"chatcmpl-{uuid.uuid4().hex[:24]}"
        created = int(time.time())
        model = model_name
        usage = None

        self._start_chunked("text/event-stream")

        def emit(obj: dict):
            self._chunk(f"data: {json.dumps(obj, ensure_ascii=False)}\n\n".encode("utf-8"))

        try:
            emit({
                "id": chunk_id, "object": "chat.completion.chunk",
                "created": created, "model": model,
                "choices": [{"index": 0, "delta": {"role": "assistant"}, "finish_reason": None}],
            })
            for raw_line in resp:
                line = raw_line.decode("utf-8", errors="replace").rstrip("\r\n")
                ev = _parse_sse_line(line)
                if ev is None:
                    continue
                t = ev.get("type")
                if t == "__done__":
                    break
                if t == "text-delta":
                    d = ev.get("delta", "")
                    if d:
                        emit({
                            "id": chunk_id, "object": "chat.completion.chunk",
                            "created": created, "model": model,
                            "choices": [{"index": 0, "delta": {"content": d}, "finish_reason": None}],
                        })
                elif t == "data-modelSelection":
                    model = (ev.get("data") or {}).get("modelId") or model
                elif t == "data-usage":
                    u = ev.get("data") or {}
                    usage = {
                        "prompt_tokens":     u.get("inputTokens", 0),
                        "completion_tokens": u.get("outputTokens", 0),
                        "total_tokens":      u.get("totalTokens", 0),
                    }

            final = {
                "id": chunk_id, "object": "chat.completion.chunk",
                "created": created, "model": model,
                "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
            }
            if usage:
                final["usage"] = usage
            emit(final)
            self._chunk(b"data: [DONE]\n\n")

        except (BrokenPipeError, ConnectionResetError):
            pass
        except Exception as e:
            self.logger.error(f"stream error: {e!r}")
            try:
                emit({"error": {"message": str(e), "type": "upstream_error"}})
                self._chunk(b"data: [DONE]\n\n")
            except Exception:
                pass
        finally:
            try: resp.close()
            except Exception: pass
            try: self._chunk_end()
            except Exception: pass

    # -------- 非流式 --------

    def _relay_json(self, resp, model_name: str):
        text_parts: list[str] = []
        model = model_name
        usage = None
        try:
            for raw_line in resp:
                line = raw_line.decode("utf-8", errors="replace").rstrip("\r\n")
                ev = _parse_sse_line(line)
                if ev is None:
                    continue
                t = ev.get("type")
                if t == "__done__":
                    break
                if t == "text-delta":
                    text_parts.append(ev.get("delta", ""))
                elif t == "data-modelSelection":
                    model = (ev.get("data") or {}).get("modelId") or model
                elif t == "data-usage":
                    u = ev.get("data") or {}
                    usage = {
                        "prompt_tokens":     u.get("inputTokens", 0),
                        "completion_tokens": u.get("outputTokens", 0),
                        "total_tokens":      u.get("totalTokens", 0),
                    }
        except (TimeoutError, OSError) as e:
            self._err(502, "upstream_error", f"stream read failed: {e!r}")
            return
        finally:
            try: resp.close()
            except Exception: pass

        self._send_json(200, {
            "id": f"chatcmpl-{uuid.uuid4().hex[:24]}",
            "object": "chat.completion",
            "created": int(time.time()),
            "model": model,
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": "".join(text_parts)},
                "finish_reason": "stop",
            }],
            "usage": usage or {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
        })


# =========================================================
# 入口
# =========================================================

def main():
    cfg_path = os.environ.get("VYBE_CONFIG", "config.ini")
    cfg = Config(cfg_path)
    logger = Logger(cfg)

    Handler.cfg = cfg
    Handler.logger = logger
    Handler.upstream = Upstream(cfg, logger)
    Handler.pool = CookiePool(cfg.cookies)

    logger.info(f"starting vybe-proxy on {cfg.host}:{cfg.port}")
    logger.info(f"upstream = {cfg.upstream_url}")
    logger.info(f"cookies = {[c.name for c in cfg.cookies]}")

    server = ThreadingHTTPServer((cfg.host, cfg.port), Handler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("shutdown")
        server.shutdown()


if __name__ == "__main__":
    main()
