"""Chat API endpoints - Multi-Agent Supervisor経由."""

import json
from typing import AsyncGenerator, Optional

import httpx
from openai import AsyncOpenAI
from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse

from app.models import ChatRequest, ChatResponse, APIResponse
from app.config import get_settings, get_oauth_token, get_databricks_host

router = APIRouter(prefix="/api/chat", tags=["chat"])

# In-memory chat history (セッションIDごと)
chat_sessions: dict[str, list[dict]] = {}


def _extract_final_text(result: dict) -> str:
    """MASレスポンスから最終回答テキストを抽出する。

    outputは複数のmessage/function_callを含む。最後のoutput_textを最終回答とする。
    <name>...</name>形式のエージェント識別子はスキップ。
    """
    output = result.get("output", [])
    final_text = ""
    for item in output:
        if item.get("type") != "message":
            continue
        for c in item.get("content", []):
            if c.get("type") == "output_text":
                text = c.get("text", "")
                stripped = text.strip()
                # エージェント名マーカーはスキップ
                if stripped.startswith("<name>") and stripped.endswith("</name>"):
                    continue
                if stripped:
                    final_text = stripped
    return final_text or "回答を取得できませんでした。"


async def _call_agent(messages: list[dict]) -> str:
    """マルチエージェントスーパーバイザーエンドポイントを呼び出す。"""
    settings = get_settings()
    host = get_databricks_host()
    token = get_oauth_token()

    if not host or not token:
        return "デモモード: エージェントエンドポイントに接続できません。"

    url = f"{host}/serving-endpoints/{settings.agent_endpoint_name}/invocations"

    async with httpx.AsyncClient(timeout=180.0) as client:
        resp = await client.post(
            url,
            json={"input": messages},
            headers={"Authorization": f"Bearer {token}"},
        )
        resp.raise_for_status()
        result = resp.json()
        return _extract_final_text(result)


async def _stream_agent(messages: list[dict]) -> AsyncGenerator[dict, None]:
    """OpenAI SDK経由でMASエンドポイントをストリーミング呼び出し。トークン到達次第即時yield。"""
    settings = get_settings()
    host = get_databricks_host()
    token = get_oauth_token()

    if not host or not token:
        yield {"type": "response.output_text.delta", "delta": "デモモード: エージェントエンドポイントに接続できません。", "step": 1}
        return

    client = AsyncOpenAI(
        api_key=token,
        base_url=f"{host}/serving-endpoints",
        timeout=180.0,
    )

    stream = await client.responses.create(
        model=settings.agent_endpoint_name,
        input=messages,
        stream=True,
    )

    async for event in stream:
        etype = getattr(event, "type", "")
        if etype == "response.output_text.delta":
            yield {
                "type": etype,
                "delta": getattr(event, "delta", ""),
                "step": getattr(event, "step", None),
            }


def _build_system_message(customer_id: Optional[str]) -> dict:
    """顧客コンテキストを含むシステムメッセージを生成する。"""
    content = "現在、営業担当者向けのCar AI Demoアシスタントとして動作しています。"
    if customer_id:
        content += f"\n\n現在表示中の顧客ID: {customer_id}。この顧客に関する質問には必ずこのIDで顧客データを参照してください。"
    return {"role": "system", "content": content}


@router.post("", response_model=ChatResponse)
async def chat(request: ChatRequest) -> ChatResponse:
    """チャットメッセージを送信してエージェントから回答を得る。"""
    try:
        session_id = request.session_id
        customer_id = request.customer_id
        message = request.message

        if session_id not in chat_sessions:
            chat_sessions[session_id] = []

        # 会話履歴にユーザーメッセージを追加
        chat_sessions[session_id].append({"role": "user", "content": message})

        # システムメッセージ + 会話履歴をエージェントに送信
        messages = [_build_system_message(customer_id)] + chat_sessions[session_id]
        response = await _call_agent(messages)

        # アシスタント応答を履歴に保存
        chat_sessions[session_id].append({"role": "assistant", "content": response})

        return ChatResponse(session_id=session_id, response=response)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/stream")
async def chat_stream(request: ChatRequest):
    """エージェント呼び出しをSSEストリームとして返す（エージェントはnon-streaming）。"""
    try:
        session_id = request.session_id
        customer_id = request.customer_id
        message = request.message

        if session_id not in chat_sessions:
            chat_sessions[session_id] = []

        chat_sessions[session_id].append({"role": "user", "content": message})
        messages = [_build_system_message(customer_id)] + chat_sessions[session_id]

        async def generate():
            try:
                step_texts: dict[int, str] = {}
                last_step: Optional[int] = None

                async for event in _stream_agent(messages):
                    etype = event.get("type", "")
                    step = event.get("step")

                    if etype == "response.output_text.delta":
                        delta = event.get("delta", "")
                        if not delta:
                            continue
                        # ステップごとに蓄積（history保存用）
                        if step is not None:
                            step_texts.setdefault(step, "")
                            step_texts[step] += delta
                            last_step = step
                        # deltaをそのままcontent SSEとして即時送信
                        yield f"data: {json.dumps({'content': delta})}\n\n"

                # historyには最終ステップのテキストのみ保存
                if last_step is not None and last_step in step_texts:
                    final_text = step_texts[last_step]
                elif step_texts:
                    final_text = step_texts[max(step_texts.keys())]
                else:
                    final_text = ""
                chat_sessions[session_id].append({"role": "assistant", "content": final_text})
                yield "data: [DONE]\n\n"
            except Exception as e:
                yield f"data: {json.dumps({'type': 'error', 'error': str(e)})}\n\n"

        return StreamingResponse(
            generate(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",   # nginxプロキシのバッファリング無効化
                "Transfer-Encoding": "chunked",
            },
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/history/{session_id}", response_model=APIResponse)
async def get_chat_history(session_id: str) -> APIResponse:
    """セッションのチャット履歴を返す。"""
    if session_id not in chat_sessions:
        return APIResponse(success=True, data=[])

    history = [msg for msg in chat_sessions[session_id] if msg["role"] != "system"]
    return APIResponse(success=True, data=history)


@router.delete("/history/{session_id}", response_model=APIResponse)
async def clear_chat_history(session_id: str) -> APIResponse:
    """セッションのチャット履歴を削除する。"""
    if session_id in chat_sessions:
        del chat_sessions[session_id]
    return APIResponse(success=True, data={"message": "Chat history cleared"})
