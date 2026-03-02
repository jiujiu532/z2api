# Repository Guidelines

## Project Structure & Module Organization
- `openai.py`: FastAPI proxy server exposing `/v1/models`, `/v1/chat/completions`, and `/v1/messages`.
- `main.py`: async `ZaiClient` for upstream `chat.z.ai` calls; use it for local protocol smoke checks.
- `claude_compat.py`: conversion helpers between Claude Messages format and OpenAI-like internal format.
- `docker-compose.yml`: container startup definition (service listens on `30016`).
- `启动服务.bat`: Windows quick-start script for local launch.
- Put new tests under `tests/` (create this directory if missing). Keep module-specific helper code near the owning module.

## Build, Test, and Development Commands
```bash
python -m venv .venv && source .venv/bin/activate
pip install fastapi uvicorn httpx httpcore
python openai.py
python main.py
docker compose up --build
```
- First two commands create an isolated environment and install runtime dependencies.
- `python openai.py` starts the local API server on `0.0.0.0:30016`.
- `python main.py` runs a direct upstream client flow for sanity checks.
- `docker compose up --build` runs the same service in a containerized environment.

## Coding Style & Naming Conventions
- Python style: 4-space indentation, explicit type hints on public functions, `snake_case` for variables/functions, `UPPER_SNAKE_CASE` for constants.
- Keep request handlers small; move parsing/translation logic into helper functions (see `claude_compat.py` patterns).
- Prefer clear async method names (`acquire`, `release`, `refresh_auth`) and short, behavior-focused comments.

## Testing Guidelines
- No committed automated test suite is present yet. Add `pytest` tests for every behavior change.
- Test file naming: `tests/test_<module>.py` (example: `tests/test_claude_compat.py`).
- Minimum local verification before merge:
  - call `GET /v1/models`
  - run one `POST /v1/chat/completions` streaming request
  - run one `POST /v1/messages` Claude-compatible request
- Target at least 90% coverage on changed modules.

## Commit & Pull Request Guidelines
- Git history is unavailable in this workspace snapshot; use Conventional Commits going forward (`feat:`, `fix:`, `refactor:`, `docs:`).
- PRs should include:
  - concise problem statement and scope
  - API contract changes with request/response examples
  - local verification commands executed
  - rollback notes for endpoint or env-var changes

## Security & Configuration Tips
- Never commit tokens, session data, or full sensitive prompts in logs.
- Control behavior via env vars (`LOG_LEVEL`, `HTTP_DEBUG`, `POOL_SIZE`, `TOKEN_MAX_AGE`) instead of hardcoding runtime values.
- When sharing debug output, redact user content and tool payloads.
