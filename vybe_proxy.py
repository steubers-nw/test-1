#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
vybe_proxy.py — vybe.build → OpenAI 兼容协议代理（多 Cookie + 多 Channel 版）

上游层级：Agent → Channel → Thread → Message
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
        cp = configparser.ConfigParser(interpolation=None)
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
            token      = cp.get(section, "session_token", fallback="").strip()
            agent_id   = cp.get(section, "agent_id",   fallback="").strip()
            channel_id = cp.get(section, "channel_id", fallback="").strip()
            user_id    = cp.get(section, "user_id",    fallback="").strip()
            user_name  = cp.get(section, "user_name",  fallback="").strip()
            user_image = cp.get(section, "user_image", fallback="").strip()
            enabled    = cp.getboolean(section, "enabled", fallback=True)
            if not token or not agent_id or not channel_id or not user_id:
                raise SystemExit(
                    f"[{section}] missing one of: session_token / agent_id / channel_id / user_id"
                )
            self.cookies.append(CookieEntry(
                name, token, agent_id, channel_id,
                user_id, user_name, user_image, enabled,
            ))

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
    """一个账户 = (session_token, agent_id, channel_id, user_id, user_name, user_image)。"""
    __slots__ = (
        "name", "session_token", "agent_id", "channel_id",
        "user_id", "user_name", "user_image", "enabled",
        "cooldown_until", "fail_count", "last_error", "_lock",
    )

    def __init__(self, name, session_token, agent_id, channel_id,
                 user_id, user_name, user_image, enabled):
        self.name = name
        self.session_token = session_token
        self.agent_id = agent_id
        self.channel_id = channel_id
        self.user_id = user_id
        self.user_name = user_name
        self.user_image = user_image
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
        with self._lock:
            self.enabled = False
            self.last_error = f"auth: {msg[:200]}"

    def mark_quota_exhausted(self, msg: str, cooldown: int):
        with self._lock:
            self.cooldown_until = time.time() + cooldown
            self.last_error = f"quota: {msg[:200]}"

    def mark_transient_fail(self, msg: str, max_fails: int, cooldown: int):
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
            "channel_id": self.channel_id,
            "user_id": self.user_id,
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

    def get(self, name):
        return self._by_name.get(name)

    def available_entries(self, preferred: str | None = None) -> list[CookieEntry]:
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


def _build_vybe_payload(thread_id: str, user_text: str, system_text: str,
                        cookie: CookieEntry) -> dict:
    text = user_text
    if system_text:
        text = f"[System instructions]\n{system_text}\n\n[User]\n{user_text}"

    user_obj: dict = {"id": cookie.user_id}
    if cookie.user_name:
        user_obj["name"] = cookie.user_name
    if cookie.user_image:
        user_obj["image"] = cookie.user_image

    return {
        "trigger": "submit-message",
        "id": thread_id,
        "message": {
            "id": str(uuid.uuid4()),
            "parts": [{"type": "text", "text": text}],
            "metadata": {
                "createdAt": int(time.time() * 1000),
                "timeZone": "Asia/Shanghai",
                "userId": cookie.user_id,
                "user": user_obj,
            },
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
class UpstreamThreadNotFoundError(Exception): pass


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

    def _headers(self, cookie: CookieEntry) -> dict:
        return {
            "Cookie": f"__Secure-better-auth.session_token={cookie.session_token}",
            "Content-Type": "application/json",
        }

    def _classify_http_error(self, e: urllib.error.HTTPError, body: str):
        """把 HTTPError 转成分类异常。"""
        if e.code == 401:
            raise UpstreamAuthError(f"session expired: {body[:200]}") from e
        if e.code in (402, 429):
            raise UpstreamQuotaError(f"quota/rate: {body[:200]}") from e
        if e.code == 404:
            try:
                obj = json.loads(body)
                err = (obj.get("error") or "").lower()
                if "thread" in err:
                    raise UpstreamThreadNotFoundError(body[:200]) from e
            except (json.JSONDecodeError, AttributeError):
                pass
        raise UpstreamGenericError(e.code, body) from e

    def create_thread(self, cookie: CookieEntry) -> str:
        """创建空 thread，返回 threadId。"""
        tid = str(uuid.uuid4())
        url = (
            f"{self.cfg.upstream_url}/api/agents/{cookie.agent_id}"
            f"/channels/{cookie.channel_id}/threads"
        )
        body = json.dumps({"id": tid, "initial": False}).encode("utf-8")
        req = urllib.request.Request(url, data=body, method="POST")
        for k, v in self._headers(cookie).items():
            req.add_header(k, v)
        try:
            resp = urllib.request.urlopen(req, timeout=30)
            resp.read()      # 读掉响应体，确保连接正常关闭
            resp.close()
            return tid
        except urllib.error.HTTPError as e:
            try:
                err_body = e.read().decode("utf-8", errors="replace")
            except Exception:
                err_body = ""
            self._classify_http_error(e, err_body)
            raise  # 不会到达
        except urllib.error.URLError as e:
            raise UpstreamGenericError(502, f"network: {e.reason}") from e

    def open_stream(self, cookie: CookieEntry, thread_id: str, payload: dict):
        url = (
            f"{self.cfg.upstream_url}/api/agents/{cookie.agent_id}"
            f"/threads/{thread_id}/chat"
        )
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        req = urllib.request.Request(url, data=body, method="POST")
        for k, v in self._headers(cookie).items():
            req.add_header(k, v)
        req.add_header("Accept", "text/event-stream")
        try:
            return urllib.request.urlopen(req, timeout=self.cfg.timeout)
        except urllib.error.HTTPError as e:
            try:
                err_body = e.read().decode("utf-8", errors="replace")
            except Exception:
                err_body = ""
            self._classify_http_error(e, err_body)
            raise  # 不会到达
        except urllib.error.URLError as e:
            raise UpstreamGenericError(502, f"network: {e.reason}") from e


# =========================================================
# Handler
# =========================================================

class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "vybe-proxy/3.0"

    cfg: Config = None
    logger: Logger = None
    upstream: Upstream = None
    pool: CookiePool = None

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

    # -------- thread 生命周期 --------

    def _resolve_thread(self, thread_key: str, cookie: CookieEntry) -> str:
        """
        返回对话在指定 cookie 下可用的 threadId。
        - 绑定存在且 cookie 一致 → 复用
        - 否则 → 调上游 create_thread 拿真实 threadId，再绑定
        """
        # 快路径
        with self._threads_lock:
            binding = self._threads.get(thread_key)
            if binding and binding[0] == cookie.name:
                return binding[1]

        # 慢路径：创建新 thread
        tid = self.upstream.create_thread(cookie)

        # 写入绑定（双重检查，避免并发重复）
        with self._threads_lock:
            binding = self._threads.get(thread_key)
            if binding and binding[0] == cookie.name:
                return binding[1]
            if len(self._threads) >= self._thread_max:
                try:
                    self._threads.pop(next(iter(self._threads)))
                except StopIteration:
                    pass
            self._threads[thread_key] = (cookie.name, tid)
        self.logger.info(f"created thread cookie={cookie.name} tid={tid[:8]}...")
        return tid

    def _clear_binding(self, thread_key: str, cookie_name: str):
        with self._threads_lock:
            binding = self._threads.get(thread_key)
            if binding and binding[0] == cookie_name:
                del self._threads[thread_key]

    def _bindings_snapshot(self, thread_key: str):
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

        binding = self._bindings_snapshot(thread_key)
        preferred = binding[0] if binding else None

        resp = self._open_with_failover(thread_key, preferred, user_text, system_text)
        if resp is None:
            return

        if stream:
            self._relay_stream(resp, model_name)
        else:
            self._relay_json(resp, model_name)

    def _open_with_failover(self, thread_key, preferred, user_text, system_text):
        candidates = self.pool.available_entries(preferred)
        if not candidates:
            self._err(503, "no_cookie",
                      "no available cookie in pool (all disabled/cooling)")
            return None

        last_error: tuple[str, int, str] | None = None

        for cookie in candidates:
            try:
                thread_id = self._resolve_thread(thread_key, cookie)
                payload = _build_vybe_payload(thread_id, user_text, system_text, cookie)
                resp = self.upstream.open_stream(cookie, thread_id, payload)
                cookie.mark_success()
                self.logger.info(
                    f"cookie={cookie.name} thread={thread_id[:8]}... ok"
                )
                return resp

            except UpstreamAuthError as e:
                cookie.mark_auth_failed(str(e))
                self._clear_binding(thread_key, cookie.name)
                self.logger.warn(f"cookie={cookie.name} auth failed: {e}")
                last_error = ("auth_error", 401, str(e))

            except UpstreamQuotaError as e:
                cookie.mark_quota_exhausted(str(e), self.cfg.cookie_cooldown)
                self.logger.warn(f"cookie={cookie.name} quota: {e}")
                last_error = ("insufficient_quota", 429, str(e))

            except UpstreamThreadNotFoundError as e:
                # 上游清理了 thread，清除绑定但不惩罚 cookie
                self._clear_binding(thread_key, cookie.name)
                self.logger.warn(
                    f"cookie={cookie.name} thread not found, binding cleared"
                )
                last_error = ("thread_not_found", 404, str(e))
                # 不标记 cookie 失败，继续尝试下一个 cookie
                # （下一个 cookie 会走 _resolve_thread 创建新 thread）

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
    logger.info(f"cookies = {[(c.name, c.agent_id, c.channel_id) for c in cfg.cookies]}")

    server = ThreadingHTTPServer((cfg.host, cfg.port), Handler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("shutdown")
        server.shutdown()


if __name__ == "__main__":
    main()
