# Codex Native Relay 插件

把 Sub2API 中 **OpenAI OAuth 账号** 的 Codex Responses 请求（`https://chatgpt.com/backend-api/codex/responses` 及 `/compact`）转发给本机运行的 `codex relay`。上游请求由 Codex CLI 自己的原生链路发出：原生 HTTP 客户端、`originator` / `User-Agent` / `version` 等原生请求头，以及（默认模式下）`codex login` 的 ChatGPT 登录和自动刷新。Sub2API 仍负责账号调度、SSE 解析、错误映射、用量统计和计费。

```text
客户端 ──/v1/responses、/v1/chat/completions──▶ Sub2API（OpenAI OAuth 账号）
                                                  │ openai.oauth.outbound_transport.v1
                                                  ▼
                                    codex-native-relay 插件进程
                                                  │ X-Codex-Relay-Secret
                                                  ▼
                                   codex relay（Codex CLI 原生客户端）──▶ chatgpt.com
```

## 1. 启动 codex relay

relay 在 Codex CLI 中实现（`david828019-oss/codex` 的 `codex relay` 子命令）。在运行 Sub2API 的机器上：

```bash
export CODEX_RELAY_SECRET="$(openssl rand -hex 32)"
codex login                     # codex 模式需要；passthrough 模式可省略
codex relay --listen 127.0.0.1:8788 --auth codex
```

| `--auth` | 上游凭据 | 适用场景 |
| --- | --- | --- |
| `codex`（默认） | relay 所在机器 `codex login` 的账号，relay 负责刷新 Token；插件不会把 Sub2API 账号 Token 发给 relay | 用一个 Codex 登录承接流量 |
| `passthrough` | 每个请求携带的 Sub2API 账号 `Authorization` 与 `ChatGPT-Account-Id`，由 Codex 原生客户端代发 | 多个 OAuth 账号都走 Codex 原生客户端 |

relay 默认只监听回环地址，所有请求必须带 `X-Codex-Relay-Secret`；未设置 `CODEX_RELAY_SECRET` 时会生成随机密钥并打印一次。

## 2. 构建和安装插件

```bash
cd plugins/codex-native-relay
go test ./... -count=1
./build.sh                                   # 未签名包，仅用于本地调试
# 生产包：先生成发布者密钥，再签名
go run ./tools/keygen -out build/keys/my-publisher
./build.sh -signing-key build/keys/my-publisher.private -key-id my-publisher-v1 \
  -output dist/codex-native-relay.s2plugin
```

在 Sub2API 配置中信任公钥：

```yaml
plugins:
  allow_unsigned: false
  trusted_publishers:
    my-publisher-v1: "BASE64_ED25519_PUBLIC_KEY"
```

然后在管理后台「插件」页上传 `.s2plugin`，在插件配置页填写 relay 地址、共享密钥和凭据模式，点击「保存并测试」，最后为 `openai / oauth` 绑定设置灰度比例并启用。

## 3. 配置项

| 字段 | 默认值 | 说明 |
| --- | --- | --- |
| `relay_url` | `http://127.0.0.1:8788` | relay 地址，只能包含协议、主机和端口 |
| `relay_secret` | 空 | 与 `CODEX_RELAY_SECRET` 一致，至少 16 个字符；为空时 Codex 请求会失败且不会发出 |
| `credential_mode` | `codex` | 必须与 relay 的 `--auth` 一致，「测试」会校验 |
| `unmatched_requests` | `direct` | 非 Codex Responses 请求（图片、文件等）：`direct` 由插件按账号代理直连，`reject` 直接拒绝 |
| `allow_remote_relay` | `false` | 允许非回环 relay 地址，此时应使用 https |
| `response_header_timeout_seconds` | `300` | 等待 relay 响应头的超时 |
| `direct_response_header_timeout_seconds` | `120` | 直连请求等待响应头的超时 |

## 4. 失败与重试语义

- relay 自身拒绝（密钥错误、路径不支持、Codex 未登录等，响应头 `X-Codex-Relay-Error: relay`）以及连接 relay 失败，返回 `request_sent=false`，Sub2API 可以切换账号重试。
- relay 已尝试上游（`X-Codex-Relay-Error: upstream`）或连接中途失败，返回 `request_sent=true`，避免重复执行。
- 上游 HTTP 错误（429、401 等）原样透传，由 Sub2API 现有错误映射处理。
