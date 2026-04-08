# Repository Guidelines

## Project Structure
- `server.py`: FastAPI proxy server exposing `/v1/models`, `/v1/chat/completions`, `/v1/messages`, and `/health`.
- `zai_client.py`: async `ZaiClient` for upstream `chat.z.ai` calls with shared HTTP/2 connection pool.
- `claude_adapter.py`: conversion helpers between Claude Messages format and OpenAI-like internal format.
- `docker-compose.yml`: container startup definition (service listens on `30016`).
- `启动服务.bat`: Windows quick-start script for local launch.

## Build, Test, and Development Commands
```bash
pip install fastapi uvicorn "httpx[http2]" httpcore h2
python server.py
docker compose up --build
```

## Coding Style
- Python 3.11+, 4-space indentation, type hints on public functions.
- `snake_case` for variables/functions, `UPPER_SNAKE_CASE` for constants.
- Keep request handlers small; move parsing/translation logic into helper functions.

## Testing
- Call `GET /v1/models`
- Run one `POST /v1/chat/completions` streaming request
- Run one `POST /v1/messages` Claude-compatible request
- Check `GET /health` for pool status

## Configuration
- Control behavior via env vars (`LOG_LEVEL`, `POOL_SIZE`, `TOKEN_MAX_AGE`, `MAX_STREAM_RETRIES`, etc.)
- Never commit tokens or session data in logs.
