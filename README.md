# vetta_proxy —— vetta.sh 模型代理的极简本地反代

> 单文件、零第三方依赖（Python 标准库）。对外是 OpenAI 兼容的 `/v1/chat/completions`，
> 对内转发到 `https://api.vetta.sh/v1/proxy/openai/v1/chat/completions`。

## 1. 它就是一层 OAI 翻译

```
Cherry Studio / Cline / NextChat / OpenAI SDK   （客户端只会说 OpenAI 协议）
        │  POST /v1/chat/completions
        ▼
   vetta_proxy  (0.0.0.0:8327)
        │  ① 剥掉上游不认的字段   ② 短名换成真实模型 id
        │  ③ 挡位归一            ④ 多密钥轮换
        ▼
   api.vetta.sh  （上游也是 OpenAI 协议）──▶  Gemini / Claude Opus
        │
        ▼
   客户端收到标准 chat.completion 或 SSE chunk
```

上游有 OpenAI 门，所以**不需要换协议**，需要的是「翻译字段」：它对未知字段一律
`400 unknown body field` 而不是忽略，客户端随手带的 `top_p` / `seed` / `reasoning_effort`
都会把请求打死。反代把入站 OAI 请求翻译成上游认得的形状，再把上游响应原样按 OAI 回给客户端；
上游回非 JSON（例如 Cloudflare 502 页面）时也会包成 `{"error":{...}}` 的 OAI 错误体。

## 2. 快速开始

```bat
:: 双击即可，窗口常驻；关窗口 = 停止服务
run.bat
```

| 项 | 值 |
|---|---|
| OpenAI base_url | `http://127.0.0.1:8327/v1` |
| API Key | 随便填（`config.ini` 的 `api_key` 留空时不校验） |
| 模型列表 | `http://127.0.0.1:8327/v1/models` |
| 健康检查 | `http://127.0.0.1:8327/healthz`（会报密钥池剩余数量） |
| 单请求汇总 | `logs/access.jsonl` |

```bash
curl http://127.0.0.1:8327/v1/chat/completions -H "Content-Type: application/json" -d '{
  "model": "4a",
  "messages": [{"role":"user","content":"hi"}]
}'
```

只需要 Python 3.9+，不装任何包。

## 3. 密钥池（多 key + 重试 + 自动摘除）

```ini
[upstream]
# 英文逗号分隔，按顺序优先；也接受换行写
keys = sk_live_aaa, sk_live_bbb, sk_live_ccc
# 同一把 key 失败先重试几次（1 = 首次 + 1 次重试）
retries_per_key = 1
retry_backoff_ms = 400
# 401/403 且重试仍失败 → 从 keys 摘掉，写进 [disabled]
auto_disable = true
```

判定规则：

| 上游状态 | 处理 |
|---|---|
| `401` / `403`（key 本身不能用） | 同一把 key 重试 `retries_per_key` 次 → 仍失败则**换下一把**，并把它摘到 `[disabled]` |
| `429` / `5xx` / 网络错误 | 同一把 key 重试 → 仍失败则换下一把，但**不摘除**（key 没坏） |
| 其它 `4xx`（`400` 等） | 请求本身的问题，**不换 key**，原样回给客户端 |

成功的 key 会被记住，下次优先用它，不会每次都从坏 key 试起。

被摘除的 key 长这样（自动写进文件末尾，注释和顺序都不动）：

```ini
[disabled]
# 自动摘除的密钥：key = 摘除时间 + 原因。
sk_live_aaa = 2026-11-02 14:33:13 401 {"error":{"type":"authentication","code":"unauthorized","message":"invalid API key"}}
```

充值或换新 key 后：删掉 `[disabled]` 里那一行，把 key 加回 `keys =`，重启即可恢复。
环境变量 `VETTA_API_KEY` 优先级最高，会用做首选 key。

## 4. 日志：控制台精简，明细落文件

**控制台**每请求只输出一行（时间 + 请求的模型 + 状态码 + 耗时），换 key / 摘 key 时多一行：

```
14:44:10  4a         200   10.90s
14:41:50  !! key sk_liv...0000 disabled (1 left)
14:41:55  4a         200   10.98s
```

**`logs/proxy-<启动时间>.log`** 是明细（纯 ASCII，cmd / PowerShell 里 `type` 也不乱码）：

```
2026-11-02 14:43:59 REQ  0cb42e 127.0.0.1 model=4a -> google/gemini-4-argon stream=0 msgs=1 effort=medium keys=1 bytes=203
2026-11-02 14:44:10 KEY  0cb42e ok sk_liv...jgb2 (attempt 1)
2026-11-02 14:44:10 DONE 0cb42e 200 chars=35 bytes=410 elapsed=10.90s key=sk_liv...jgb2 upstream_req=req_p44jvj75zft025fdhapq42qyeb
```

重试与摘除也都在这里，能看出卡在哪一层：

```
14:41:49 KEY  8c0222 sk_liv...0000 -> 401 (attempt 1) {"error":{"code":"unauthorized",...}}
14:41:50 KEY  8c0222 sk_liv...0000 -> 401 (attempt 2) {...}
14:41:50 KEY  sk_liv...0000 disabled -> [disabled]: 401 {...} (left=1)
14:41:55 KEY  8c0222 ok sk_liv...jgb2 (attempt 1)
14:41:55 DONE 8c0222 200 chars=32 bytes=407 elapsed=10.98s key=sk_liv...jgb2 upstream_req=req_gv1ntqne4w...
```

其余落点：

| 文件 | 内容 |
|---|---|
| `logs/access.jsonl` | 一行一请求的结构化汇总（model / status / msgs / ttft / elapsed / key / upstream_req / client_ip） |
| `logs/bodies/<时间>-<rid>.json` | 该请求的完整请求体与响应体（含对话原文），`[logging] log_bodies = false` 可关 |

## 5. 模型清单

`/v1/models` 只列下面这些短名；真实上游 id 也能直接当 `model` 传（只是不进清单）。

### Gemini（3.5-flash 系列 ~ 4-argon）

| 短名 | 上游 id | 备注 |
|---|---|---|
| `3.5fl` | `google/gemini-3.5-flash-lite` | |
| `3.5f` | `google/gemini-3.5-flash` | |
| `3.6f` | `google/gemini-3.6-flash` | |
| `3.7f` | `google/gemini-3.7-flash` | |
| `3.8f` | `google/gemini-3.8-flash` | |
| `4a` | `google/gemini-4-argon` | 实测 200，默认模型 |

### Claude Opus（4.5 ~ 5.5）

| 短名 | 上游 id | 上游公布的挡位 |
|---|---|---|
| `4.5o` | `anthropic/claude-opus-4.5` | 无（`efforts: []`） |
| `4.6o` | `anthropic/claude-opus-4.6` | low / medium / high |
| `4.7o` | `anthropic/claude-opus-4.7` | low / medium / high |
| `4.8o` | `anthropic/claude-opus-4.8` | low / medium / high |
| `5o` | `anthropic/claude-opus-5` | low / medium / high |
| `5.5o` | `anthropic/claude-opus-5.5` | low / medium / high，实测 200 |

区间之外的模型（`gemini-3.1-*`、`gemini-2.5-*`、`claude-opus-4.1`、haiku / sonnet 全系）都不在清单里。

> **`:batch` 后缀不可用**：`google/gemini-4-argon:batch` 这类批量变体走实时调用会被上游
> 400 `validation_failed: the model could not accept this request`，清单里一律用不带后缀的 id。

## 6. 思考挡位：上游没有这个接口

上游 proxy 的请求体**没有**任何思考挡位字段。同一条 `hi` 实测：

| 传法 | 结果 |
|---|---|
| `reasoning_effort: "high"` | 400 `unknown body field: reasoning_effort` |
| `effort: "high"` | 400 `unknown body field: effort` |
| `output_config: {"effort":"high"}` | 400 `unknown body field: output_config` |
| `thinking: {"type":"enabled","budget_tokens":1024}` | 400 `unknown body field: thinking` |
| `model: {"id":..., "effort":"high"}` | 400 `model: expected string, received object` |
| 模型 id 加 `:high` | 400 `unknown model` |

官方 SDK 文档同样写明 `ProxyMessageCreate` 里没有 `thinking` / `temperature` 等字段。
上游的 `effort` 只存在于 **agent / session** 配置（`model: { id, effort }`），proxy 这条路上拿不到。

所以反代的做法是「接收 + 归一 + 二选一落地」：

```ini
[thinking]
default_effort = high     ; 客户端没传挡位时按这个算
mode = strip              ; strip = 剥掉参数（默认）；model = 按挡位换模型
model_low  =
model_medium = anthropic/claude-opus-4.6
model_high = anthropic/claude-opus-5.5
```

- 归一：`reasoning_effort` / `thinking_level` / `thinkingLevel` / `effort`，以及 Anthropic 的
  `thinking.budget_tokens`（≤2048 → low，≤8192 → medium，更大 → high）统一成 `low|medium|high`。
- `strip`：挡位只进日志（`effort=high`），不上送，避免客户端因此吃 400。
- `model`：挡位换成 `model_low/medium/high` 配的模型 —— **这是唯一真实可见的"挡位"**。
- 另外会把挡位塞进 `Vetta-Effort` 头（默认开）。上游文档没列这个头、实测无效，只为将来兼容。

## 7. 字段白名单

只放行这些，其余一律剥掉：

```
model, messages, stream, max_tokens, max_completion_tokens, temperature, tools
```

`temperature` 上游接受但不转发（模型跑自己的默认值）；`max_*` 超过 32768 会被压到上限。

## 8. 流式

`stream: true` 时按 SSE 原样转发上游 chunk（`data: {...}` … `data: [DONE]`），
用 `Transfer-Encoding: chunked` 逐块 flush，不做整段缓冲。实测首帧与上游一致。

## 9. 已实测的通路

| 项目 | 结果 |
|---|---|
| `4a` 非流式（带 `top_p` / `reasoning_effort` 干扰） | 200，字段被清洗后正常出字 |
| `4a` 直连流式 | 200，标准 SSE chunk + usage + `[DONE]` |
| `5.5o` 经反代流式（带 `thinking`） | 200，`thinking` 被剥离，SSE 正常 |
| 假 key + 真 key | 401 重试 2 次 → 摘除到 `[disabled]` → 换 key 200 |
| 限流 | 上游响应头 `ratelimit-limit: 3000`（每分钟） |

## 10. 已知限制

- 只做 OpenAI 入站。Anthropic 原生客户端（Claude Code 等）直接把 base_url 指到
  `https://api.vetta.sh/v1/proxy/anthropic` 即可，不必经过本服务。
- 图片 / 文档 content block 上游不支持，会被拒（本服务原样透传，错误看得见）。
- 余额不足时上游返回 402 `insufficient_credits`，属于组织级问题，换 key 无效，本服务原样透传。
- 不做请求排队与并发合并：上游回什么就回什么，方便排查。
