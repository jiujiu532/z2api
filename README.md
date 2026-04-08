# Z.ai OpenAI 兼容代理

将 [chat.z.ai](https://chat.z.ai) 的接口转换为 **OpenAI** 和 **Anthropic Claude** 兼容格式的代理服务。

## 功能特性

- **双协议兼容** — 同时支持 OpenAI `/v1/chat/completions` 和 Claude `/v1/messages`
- **多账号池** — 自动管理游客会话，按负载动态扩缩容
- **Function Calling** — 兼容 OpenAI 工具调用格式
- **流式输出** — 完整 SSE 流式和非流式两种模式
- **高并发优化** — 共享连接池 + HTTP/2 多路复用 + 全局并发限流 + 自动重试
- **健康监控** — `/health` 端点实时查看池状态

## 项目结构

```
├── server.py           # FastAPI 代理服务（主入口）
├── zai_client.py       # Z.ai 上游客户端（认证、签名、SSE 流）
├── claude_adapter.py   # Claude Messages 协议适配层
├── Dockerfile          # Docker 镜像构建
├── docker-compose.yml  # Docker Compose 部署配置
├── 启动服务.bat         # Windows 本地快速启动
└── README.md
```

## 快速开始

### Docker 部署（推荐）

```bash
docker compose up --build -d
```

服务默认监听 `0.0.0.0:30016`。

### 本地运行

```bash
pip install fastapi uvicorn "httpx[http2]" httpcore h2
python server.py
```

或双击 `启动服务.bat`。

## API 端点

| 端点 | 方法 | 说明 |
|------|------|------|
| `/v1/models` | GET | 获取可用模型列表 |
| `/v1/chat/completions` | POST | OpenAI 格式对话补全 |
| `/v1/messages` | POST | Claude 格式对话补全 |
| `/health` | GET | 健康检查 + 池状态监控 |

## 配置参数

通过环境变量配置，可在 `docker-compose.yml` 中设置：

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `POOL_MIN_SIZE` | 5 | 账号池最小大小 |
| `POOL_MAX_SIZE` | 72 | 账号池最大大小 |
| `POOL_TARGET_INFLIGHT_PER_ACCOUNT` | 2 | 每账号目标并发数 |
| `TOKEN_MAX_AGE` | 480 | 账号令牌过期时间（秒） |
| `MAX_STREAM_RETRIES` | 3 | 流式请求最大重试次数 |
| `GLOBAL_CONCURRENCY_LIMIT` | 96 | 全局最大并发请求数 |
| `UPSTREAM_FIRST_EVENT_TIMEOUT` | 60 | 首事件超时时间（秒） |
| `ENABLE_THINKING` | 1 | 是否启用思考链输出 |
| `LOG_LEVEL` | INFO | 日志级别 |

## 使用示例

```bash
# 获取模型列表
curl http://localhost:30016/v1/models

# OpenAI 格式请求
curl http://localhost:30016/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{"model":"glm-5","stream":true,"messages":[{"role":"user","content":"你好"}]}'

# 健康检查
curl http://localhost:30016/health
```
