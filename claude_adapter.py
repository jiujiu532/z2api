"""Anthropic Claude Messages API (/v1/messages) helpers.

Converts between Anthropic Claude native format and the internal
OpenAI-like format already used by openai.py.
"""

from __future__ import annotations

import json
import uuid
from typing import Any


# ── Message conversion ───────────────────────────────────────────────


def claude_messages_to_openai(system: Any, messages: list[dict]) -> list[dict]:
    """Convert Anthropic Claude messages to OpenAI message format."""
    out: list[dict] = []

    # System is a top-level field in Claude format
    if system:
        if isinstance(system, str):
            out.append({"role": "system", "content": system})
        elif isinstance(system, list):
            texts = [
                b.get("text", "")
                for b in system
                if isinstance(b, dict) and b.get("type") == "text"
            ]
            if texts:
                out.append({"role": "system", "content": "\n".join(texts)})

    for msg in messages:
        role = msg.get("role", "user")
        content = msg.get("content", "")

        # Assistant with content blocks (may include tool_use)
        if role == "assistant" and isinstance(content, list):
            text_parts: list[str] = []
            tool_calls: list[dict] = []
            for block in content:
                if not isinstance(block, dict):
                    continue
                bt = block.get("type")
                if bt == "text":
                    text_parts.append(block.get("text", ""))
                elif bt == "tool_use":
                    tool_calls.append({
                        "id": block.get("id", f"call_{uuid.uuid4().hex[:24]}"),
                        "type": "function",
                        "function": {
                            "name": block.get("name", ""),
                            "arguments": json.dumps(block.get("input", {}), ensure_ascii=False),
                        },
                    })
            omsg: dict = {"role": "assistant", "content": " ".join(text_parts).strip() or None}
            if tool_calls:
                omsg["tool_calls"] = tool_calls
            out.append(omsg)
            continue

        # User with tool_result blocks
        if role == "user" and isinstance(content, list):
            has_tool_result = any(
                isinstance(b, dict) and b.get("type") == "tool_result" for b in content
            )
            if has_tool_result:
                for block in content:
                    if not isinstance(block, dict):
                        continue
                    bt = block.get("type")
                    if bt == "tool_result":
                        rc = block.get("content", "")
                        if isinstance(rc, str):
                            rt = rc
                        elif isinstance(rc, list):
                            rt = " ".join(
                                s.get("text", "") for s in rc
                                if isinstance(s, dict) and s.get("type") == "text"
                            )
                        else:
                            rt = str(rc)
                        out.append({"role": "tool", "tool_call_id": block.get("tool_use_id", ""), "content": rt})
                    elif bt == "text":
                        out.append({"role": "user", "content": block.get("text", "")})
                continue

        # Default: extract text
        out.append({"role": role, "content": _extract_text(content)})

    return out


def claude_tools_to_openai(tools: list[dict] | None) -> list[dict] | None:
    """Convert Anthropic tool definitions to OpenAI format."""
    if not tools:
        return None
    out = [
        {
            "type": "function",
            "function": {
                "name": t.get("name", ""),
                "description": t.get("description", ""),
                "parameters": t.get("input_schema", {}),
            },
        }
        for t in tools
        if isinstance(t, dict)
    ]
    return out or None


def claude_tool_choice_prompt(tool_choice: Any) -> str:
    if not isinstance(tool_choice, dict):
        return ""
    tc_type = tool_choice.get("type", "auto")
    if tc_type == "any":
        return "\nIMPORTANT: You MUST call at least one tool in your next response."
    if tc_type == "tool":
        name = tool_choice.get("name", "")
        if name:
            return f"\nIMPORTANT: You MUST call this tool: {name}"
    return ""


# ── Response builders ────────────────────────────────────────────────


def make_claude_id() -> str:
    return f"msg_{uuid.uuid4().hex[:24]}"


def build_tool_call_blocks(
    tool_calls: list[dict],
) -> list[dict]:
    """Convert internal tool call dicts to Claude content blocks."""
    blocks = []
    for tc in tool_calls:
        fn = tc.get("function", {}) if isinstance(tc.get("function"), dict) else {}
        args_str = fn.get("arguments", "{}")
        try:
            args_obj = json.loads(args_str) if isinstance(args_str, str) else args_str
        except Exception:
            args_obj = {}
        blocks.append({
            "type": "tool_use",
            "id": tc.get("id", f"toolu_{uuid.uuid4().hex[:20]}").replace("call_", "toolu_"),
            "name": fn.get("name", ""),
            "input": args_obj,
        })
    return blocks


def build_non_stream_response(
    msg_id: str,
    model: str,
    reasoning_parts: list[str],
    answer_text: str,
    tool_calls: list[dict] | None,
    input_tokens: int,
    output_tokens: int,
) -> dict:
    """Build a complete Anthropic non-streaming response."""
    content: list[dict] = []
    if reasoning_parts:
        content.append({"type": "thinking", "thinking": "".join(reasoning_parts)})
    if answer_text:
        content.append({"type": "text", "text": answer_text})
    elif not tool_calls:
        content.append({"type": "text", "text": ""})
    if tool_calls:
        content.extend(build_tool_call_blocks(tool_calls))

    return {
        "id": msg_id,
        "type": "message",
        "role": "assistant",
        "content": content,
        "model": model,
        "stop_reason": "tool_use" if tool_calls else "end_turn",
        "stop_sequence": None,
        "usage": {"input_tokens": input_tokens, "output_tokens": output_tokens},
    }


# ── SSE event helpers ────────────────────────────────────────────────


def sse(event: str, data: dict) -> str:
    return f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"


def sse_message_start(msg_id: str, model: str, input_tokens: int) -> str:
    return sse("message_start", {
        "type": "message_start",
        "message": {
            "id": msg_id, "type": "message", "role": "assistant",
            "content": [], "model": model,
            "stop_reason": None, "stop_sequence": None,
            "usage": {"input_tokens": input_tokens, "output_tokens": 0},
        },
    })


def sse_ping() -> str:
    return sse("ping", {"type": "ping"})


def sse_content_block_start(index: int, block: dict) -> str:
    return sse("content_block_start", {"type": "content_block_start", "index": index, "content_block": block})


def sse_content_block_delta(index: int, delta: dict) -> str:
    return sse("content_block_delta", {"type": "content_block_delta", "index": index, "delta": delta})


def sse_content_block_stop(index: int) -> str:
    return sse("content_block_stop", {"type": "content_block_stop", "index": index})


def sse_message_delta(stop_reason: str, output_tokens: int) -> str:
    return sse("message_delta", {
        "type": "message_delta",
        "delta": {"stop_reason": stop_reason, "stop_sequence": None},
        "usage": {"output_tokens": output_tokens},
    })


def sse_message_stop() -> str:
    return sse("message_stop", {"type": "message_stop"})


def sse_error(error_type: str, message: str) -> str:
    return sse("error", {"type": "error", "error": {"type": error_type, "message": message}})


# ── Private ──────────────────────────────────────────────────────────


def _extract_text(content: object) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return " ".join(
            str(b.get("text", ""))
            for b in content
            if isinstance(b, dict) and b.get("type") == "text"
        ).strip()
    return str(content) if content else ""
