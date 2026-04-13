"""マイページ API - 営業担当者の実績確認 + Genie チャット."""

import asyncio
import calendar
import json
from typing import Optional

import httpx
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from car_ai_demo.backend.config import get_settings, get_oauth_token, get_databricks_host, get_full_table_name
from car_ai_demo.backend.database import db
from car_ai_demo.backend.llm import llm

router = APIRouter(prefix="/api/mypage", tags=["mypage"])

SALES_REPS = [
    {"id": "SR001", "name": "鈴木 一郎"},
    {"id": "SR002", "name": "高橋 健太"},
    {"id": "SR003", "name": "山本 美咲"},
    {"id": "SR004", "name": "田村 直樹"},
]

# デモモード用フォールバックデータ
_DEMO_STATS = {
    "鈴木 一郎": {
        "current_month": {"total": 82, "contracted": 26, "lost": 44, "in_progress": 12, "contract_rate": 31.7, "avg_amount": 2850000},
        "rate_diff_from_last_month": 2.1,
        "loss_reasons": [
            {"loss_reason": "予算超過", "cnt": 17},
            {"loss_reason": "競合他社で成約", "cnt": 13},
            {"loss_reason": "タイミングが合わない", "cnt": 8},
            {"loss_reason": "保留・検討中", "cnt": 4},
            {"loss_reason": "条件不一致", "cnt": 2},
        ],
        "vehicle_breakdown": [
            {"vehicle_category": "ミニバン", "total": 32, "contracted": 12, "rate": 37.5},
            {"vehicle_category": "SUV",     "total": 20, "contracted": 6,  "rate": 30.0},
            {"vehicle_category": "セダン",  "total": 14, "contracted": 4,  "rate": 28.6},
            {"vehicle_category": "コンパクト", "total": 10, "contracted": 3, "rate": 30.0},
            {"vehicle_category": "軽自動車", "total": 6,  "contracted": 1,  "rate": 16.7},
        ],
    },
    "高橋 健太": {
        "current_month": {"total": 75, "contracted": 19, "lost": 44, "in_progress": 12, "contract_rate": 25.3, "avg_amount": 3100000},
        "rate_diff_from_last_month": -0.8,
        "loss_reasons": [
            {"loss_reason": "競合他社で成約", "cnt": 18},
            {"loss_reason": "予算超過", "cnt": 12},
            {"loss_reason": "保留・検討中", "cnt": 8},
            {"loss_reason": "タイミングが合わない", "cnt": 5},
            {"loss_reason": "条件不一致", "cnt": 1},
        ],
        "vehicle_breakdown": [
            {"vehicle_category": "セダン",  "total": 28, "contracted": 10, "rate": 35.7},
            {"vehicle_category": "SUV",     "total": 18, "contracted": 4,  "rate": 22.2},
            {"vehicle_category": "ミニバン", "total": 14, "contracted": 3,  "rate": 21.4},
            {"vehicle_category": "コンパクト", "total": 10, "contracted": 2, "rate": 20.0},
            {"vehicle_category": "軽自動車", "total": 5,  "contracted": 0,  "rate": 0.0},
        ],
    },
    "山本 美咲": {
        "current_month": {"total": 71, "contracted": 20, "lost": 39, "in_progress": 12, "contract_rate": 28.2, "avg_amount": 2420000},
        "rate_diff_from_last_month": 3.5,
        "loss_reasons": [
            {"loss_reason": "保留・検討中", "cnt": 14},
            {"loss_reason": "予算超過", "cnt": 12},
            {"loss_reason": "競合他社で成約", "cnt": 8},
            {"loss_reason": "条件不一致", "cnt": 3},
            {"loss_reason": "タイミングが合わない", "cnt": 2},
        ],
        "vehicle_breakdown": [
            {"vehicle_category": "SUV",     "total": 30, "contracted": 11, "rate": 36.7},
            {"vehicle_category": "コンパクト", "total": 15, "contracted": 4, "rate": 26.7},
            {"vehicle_category": "ミニバン", "total": 12, "contracted": 3,  "rate": 25.0},
            {"vehicle_category": "軽自動車", "total": 8,  "contracted": 2,  "rate": 25.0},
            {"vehicle_category": "セダン",  "total": 6,  "contracted": 0,  "rate": 0.0},
        ],
    },
    "田村 直樹": {
        "current_month": {"total": 68, "contracted": 16, "lost": 39, "in_progress": 13, "contract_rate": 23.5, "avg_amount": 1680000},
        "rate_diff_from_last_month": -1.2,
        "loss_reasons": [
            {"loss_reason": "予算超過", "cnt": 15},
            {"loss_reason": "タイミングが合わない", "cnt": 11},
            {"loss_reason": "保留・検討中", "cnt": 8},
            {"loss_reason": "競合他社で成約", "cnt": 4},
            {"loss_reason": "条件不一致", "cnt": 1},
        ],
        "vehicle_breakdown": [
            {"vehicle_category": "軽自動車", "total": 26, "contracted": 7,  "rate": 26.9},
            {"vehicle_category": "コンパクト", "total": 20, "contracted": 5, "rate": 25.0},
            {"vehicle_category": "ミニバン", "total": 12, "contracted": 3,  "rate": 25.0},
            {"vehicle_category": "SUV",     "total": 7,  "contracted": 1,  "rate": 14.3},
            {"vehicle_category": "セダン",  "total": 3,  "contracted": 0,  "rate": 0.0},
        ],
    },
}

# Genie チャットセッション（conversation_id を保持）
_genie_sessions: dict[str, str] = {}  # session_id -> conversation_id


@router.get("/reps")
async def list_sales_reps():
    """担当者一覧をDBから返す（ALLオプション付き）。"""
    try:
        table = get_full_table_name("sv_customers")
        rows = await db.execute_query(
            f"SELECT DISTINCT sales_rep_name FROM {table} WHERE sales_rep_name IS NOT NULL ORDER BY sales_rep_name"
        )
        reps = [{"id": r["sales_rep_name"], "name": r["sales_rep_name"]} for r in rows]
    except Exception:
        reps = SALES_REPS
    all_option = [{"id": "ALL", "name": "ALL"}]
    return {"success": True, "data": all_option + reps}


async def _generate_loss_actions(
    sales_rep_name: str,
    loss_reasons: list[dict],
    vehicle_breakdown: list[dict],
    same_period_diff: float | None,
) -> dict[str, str]:
    """失注理由ごとの改善アクションをClaudeで一括生成する。"""
    try:
        vehicle_summary = ", ".join(
            f"{v['vehicle_category']}({v['contracted']}/{v['total']}件 成約率{v['rate']}%)"
            for v in vehicle_breakdown
        )
        loss_list = "\n".join(
            f"- {r['loss_reason']}: {r['cnt']}件"
            for r in loss_reasons
        )
        pace_note = f"先月同時点比 {same_period_diff:+.1f}%" if same_period_diff is not None else ""

        prompt = f"""あなたは中古車販売の営業コーチです。
営業担当「{sales_rep_name}」の今月の失注データをもとに、各失注理由に対して考えられる仮説と対策の方向性を1文で示してください。

【今月の失注理由】
{loss_list}

【車種別成約率（今月）】
{vehicle_summary}

【ペース】{pace_note}

以下のJSON形式で出力してください（他のテキスト不要）:
{{"失注理由1": "仮説・対策案（1文）", "失注理由2": "..."}}

ルール:
- 1文30〜50文字以内
- 断定・命令形は禁止（「〜すべき」「〜しましょう」はNG）
- 「〜が一因かもしれません」「〜を検討する余地があります」「〜という仮説が考えられます」など、仮説・示唆のトーンにする
- 車種名・価格帯など具体的な要素を含める
"""
        response = await llm.chat(
            messages=[{"role": "user", "content": prompt}],
            max_tokens=400,
            temperature=0.3,
        )
        text = response.strip()
        # JSON部分を抽出
        start, end = text.find("{"), text.rfind("}") + 1
        if start >= 0 and end > start:
            return json.loads(text[start:end])
    except Exception as e:
        print(f"[loss_actions] generation failed: {e}")
    return {}


@router.get("/stats")
async def get_mypage_stats(sales_rep_name: str = Query(...)):
    """担当者の今月実績・失注理由・車両カテゴリ別成約率を返す。"""
    full_table = get_full_table_name("sv_sales_results")
    is_all = sales_rep_name == "ALL"
    rep_filter = "" if is_all else f"WHERE sales_rep_name = '{sales_rep_name}'"
    rep_and = "" if is_all else f"AND sales_rep_name = '{sales_rep_name}'"

    monthly_q = f"""
        SELECT
            date_trunc('month', sale_date) AS month,
            COUNT(*) AS total,
            SUM(CASE WHEN outcome = '成約'   THEN 1 ELSE 0 END) AS contracted,
            SUM(CASE WHEN outcome = '失注'   THEN 1 ELSE 0 END) AS lost,
            SUM(CASE WHEN outcome = '商談中' THEN 1 ELSE 0 END) AS in_progress,
            ROUND(SUM(CASE WHEN outcome = '成約' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS contract_rate,
            ROUND(AVG(CASE WHEN outcome = '成約' THEN sale_price END)) AS avg_amount
        FROM {full_table}
        {rep_filter}
        GROUP BY 1 ORDER BY 1 DESC LIMIT 2
    """
    customers_table = get_full_table_name("sv_customers")
    customers_rep_and = "" if is_all else f"AND sales_rep_name = '{sales_rep_name}'"
    loss_q = f"""
        SELECT loss_reason, COUNT(*) AS cnt
        FROM {customers_table}
        WHERE stage = '失注' {customers_rep_and}
          AND loss_reason IS NOT NULL AND loss_reason != ''
        GROUP BY 1 ORDER BY 2 DESC
    """
    vehicle_q = f"""
        SELECT
            vehicle_category,
            COUNT(*) AS total,
            SUM(CASE WHEN outcome = '成約' THEN 1 ELSE 0 END) AS contracted,
            ROUND(SUM(CASE WHEN outcome = '成約' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS rate
        FROM {full_table}
        WHERE date_trunc('month', sale_date) = (
            SELECT date_trunc('month', MAX(sale_date)) FROM {full_table} {rep_filter}
        ) {rep_and}
        GROUP BY 1 ORDER BY rate DESC
    """

    daily_q = f"""
        SELECT
            date_trunc('month', sale_date) AS sale_month,
            DAY(sale_date)                 AS day_num,
            SUM(CASE WHEN outcome = '成約' THEN 1 ELSE 0 END) AS contracted
        FROM {full_table}
        WHERE date_trunc('month', sale_date) >= (
            SELECT add_months(date_trunc('month', MAX(sale_date)), -1)
            FROM {full_table} {rep_filter}
        ) {rep_and}
        GROUP BY 1, 2
        ORDER BY 1, 2
    """

    monthly, loss_reasons, vehicle_breakdown, daily_rows = await asyncio.gather(
        db.execute_query(monthly_q),
        db.execute_query(loss_q),
        db.execute_query(vehicle_q),
        db.execute_query(daily_q),
    )

    # 日別データを月ごとに集計
    by_month: dict[str, dict[int, int]] = {}
    for row in daily_rows:
        m = str(row["sale_month"])[:7]
        d = int(row["day_num"])
        by_month.setdefault(m, {})[d] = int(row.get("contracted") or 0)

    months_sorted = sorted(by_month.keys())
    cur_m = months_sorted[-1] if months_sorted else None
    prev_m = months_sorted[-2] if len(months_sorted) >= 2 else None
    cur_days = by_month.get(cur_m, {})
    prev_days = by_month.get(prev_m, {})
    cur_max_day = max(cur_days.keys(), default=0)
    prev_max_day = max(prev_days.keys(), default=0)

    # 日別累積トレンド（今月 vs 先月）
    all_max = max(cur_max_day, prev_max_day)
    daily_trend = []
    cur_cum = prev_cum = 0
    for d in range(1, all_max + 1):
        cur_cum += cur_days.get(d, 0)
        prev_cum += prev_days.get(d, 0)
        point: dict = {"day": d}
        if d <= cur_max_day:
            point["current"] = cur_cum
        if d <= prev_max_day:
            point["last_month"] = prev_cum
        daily_trend.append(point)

    # 先月同時点比（今月の最終日時点まで）
    cur_to_date = sum(cur_days.get(d, 0) for d in range(1, cur_max_day + 1))
    prev_to_same = sum(prev_days.get(d, 0) for d in range(1, cur_max_day + 1))
    same_period_diff = round((cur_to_date - prev_to_same) / prev_to_same * 100, 1) if prev_to_same > 0 else None

    # 月末着地予測
    projected_total = None
    if cur_m and cur_max_day > 0:
        year, month = int(cur_m[:4]), int(cur_m[5:7])
        days_in_month = calendar.monthrange(year, month)[1]
        projected_total = round(cur_to_date / cur_max_day * days_in_month)
    last_month_total = sum(prev_days.values()) if prev_days else None

    # DBから取れた場合
    if monthly:
        current = monthly[0]
        previous = monthly[1] if len(monthly) > 1 else {}
        rate_diff = None
        if current.get("contract_rate") is not None and previous.get("contract_rate") is not None:
            rate_diff = round(float(current["contract_rate"]) - float(previous["contract_rate"]), 1)

        return {
            "success": True,
            "data": {
                "current_month": current,
                "rate_diff_from_last_month": rate_diff,
                "same_period_diff": same_period_diff,
                "projected_total": projected_total,
                "last_month_total": last_month_total,
                "daily_trend": daily_trend,
                "loss_reasons": loss_reasons,
                "vehicle_breakdown": vehicle_breakdown,
            },
        }

    # デモモードフォールバック
    demo = _DEMO_STATS.get(sales_rep_name)
    if demo:
        return {"success": True, "data": demo}

    return {"success": True, "data": {
        "current_month": {},
        "rate_diff_from_last_month": None,
        "loss_reasons": [],
        "vehicle_breakdown": [],
    }}


@router.get("/loss-actions")
async def get_loss_actions(sales_rep_name: str = Query(...)):
    """失注理由ごとのAI改善アクションを非同期で返す。"""
    full_table = get_full_table_name("sv_sales_results")
    is_all = sales_rep_name == "ALL"
    rep_filter = "" if is_all else f"WHERE sales_rep_name = '{sales_rep_name}'"
    rep_and = "" if is_all else f"AND sales_rep_name = '{sales_rep_name}'"

    customers_table = get_full_table_name("sv_customers")
    customers_rep_and = "" if is_all else f"AND sales_rep_name = '{sales_rep_name}'"
    loss_q = f"""
        SELECT loss_reason, COUNT(*) AS cnt
        FROM {customers_table}
        WHERE stage = '失注' {customers_rep_and}
          AND loss_reason IS NOT NULL AND loss_reason != ''
        GROUP BY 1 ORDER BY 2 DESC
    """
    vehicle_q = f"""
        SELECT
            vehicle_category,
            COUNT(*) AS total,
            SUM(CASE WHEN outcome = '成約' THEN 1 ELSE 0 END) AS contracted,
            ROUND(SUM(CASE WHEN outcome = '成約' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS rate
        FROM {full_table}
        WHERE date_trunc('month', sale_date) = (
            SELECT date_trunc('month', MAX(sale_date)) FROM {full_table} {rep_filter}
        ) {rep_and}
        GROUP BY 1 ORDER BY rate DESC
    """
    try:
        loss_reasons, vehicle_breakdown = await asyncio.gather(
            db.execute_query(loss_q),
            db.execute_query(vehicle_q),
        )
        actions = await _generate_loss_actions(
            sales_rep_name=sales_rep_name,
            loss_reasons=loss_reasons,
            vehicle_breakdown=vehicle_breakdown,
            same_period_diff=None,
        )
    except Exception as e:
        print(f"[loss-actions] error: {e}")
        actions = {}
    return {"success": True, "data": actions}


class MypageChatRequest(BaseModel):
    session_id: str
    sales_rep_name: str
    message: str


async def _genie_start_or_continue(
    host: str, token: str, space_id: str, session_id: str, message: str
) -> tuple[str, str]:
    """Genie に質問を送り (conversation_id, message_id) を返す。"""
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    conv_id = _genie_sessions.get(session_id)
    if conv_id:
        # 既存会話を継続
        url = f"{host}/api/2.0/genie/spaces/{space_id}/conversations/{conv_id}/messages"
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(url, json={"content": message}, headers=headers)
            resp.raise_for_status()
            data = resp.json()
        return conv_id, data["id"]
    else:
        # 新規会話
        url = f"{host}/api/2.0/genie/spaces/{space_id}/start-conversation"
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(url, json={"content": message}, headers=headers)
            resp.raise_for_status()
            data = resp.json()
        conv_id = data["conversation_id"]
        _genie_sessions[session_id] = conv_id
        return conv_id, data["message"]["id"]


async def _genie_poll(
    host: str, token: str, space_id: str, conv_id: str, msg_id: str, timeout: int = 180
) -> list[dict]:
    """Genie のメッセージが COMPLETED になるまでポーリングし、結果アイテムのリストを返す。

    各アイテム:
      {"type": "text", "content": "..."}
      {"type": "table", "columns": [...], "rows": [[...], ...]}
    """
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{host}/api/2.0/genie/spaces/{space_id}/conversations/{conv_id}/messages/{msg_id}"
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout

    async with httpx.AsyncClient(timeout=60.0) as client:
        while loop.time() < deadline:
            resp = await client.get(url, headers=headers)
            resp.raise_for_status()
            data = resp.json()
            status = data.get("status", "")

            if status == "COMPLETED":
                items: list[dict] = []
                for att in data.get("attachments", []):
                    if "text" in att and att["text"].get("content"):
                        items.append({"type": "text", "content": att["text"]["content"]})
                    elif "query" in att:
                        q = att["query"]
                        if q.get("description"):
                            items.append({"type": "text", "content": q["description"]})
                        # テーブルデータを取得
                        att_id = att.get("attachment_id", "")
                        if att_id:
                            result_url = (
                                f"{host}/api/2.0/genie/spaces/{space_id}"
                                f"/conversations/{conv_id}/messages/{msg_id}"
                                f"/attachments/{att_id}/query-result"
                            )
                            try:
                                r = await client.get(result_url, headers=headers)
                                if r.status_code == 200:
                                    qr = r.json()
                                    sr = qr.get("statement_response", {})
                                    cols = [
                                        c["name"]
                                        for c in sr.get("manifest", {}).get("schema", {}).get("columns", [])
                                    ]
                                    rows = sr.get("result", {}).get("data_array", [])
                                    if cols and rows:
                                        items.append({"type": "table", "columns": cols, "rows": rows[:50]})
                            except Exception:
                                pass
                return items if items else [{"type": "text", "content": "（回答を取得できませんでした）"}]

            if status in ("ERROR", "FAILED"):
                raise RuntimeError(data.get("error", f"Genie returned {status}"))

            await asyncio.sleep(2)

    raise TimeoutError("Genie response timeout")


@router.get("/debug/genie")
async def debug_genie():
    """Genie接続テスト用デバッグエンドポイント（一時）"""
    settings = get_settings()
    host = get_databricks_host()
    token = get_oauth_token()
    space_id = settings.sales_mypage_genie_space_id
    result = {
        "host": host,
        "has_token": bool(token),
        "token_prefix": token[:20] + "..." if token else None,
        "space_id": space_id,
    }
    if host and token and space_id:
        try:
            # フルフローをテスト
            import uuid as _uuid
            session_id = str(_uuid.uuid4())
            conv_id, msg_id = await _genie_start_or_continue(host, token, space_id, session_id, "鈴木一郎の成約率は？")
            result["start_ok"] = True
            result["conv_id"] = conv_id
            result["msg_id"] = msg_id
            # ポーリングテスト（短いタイムアウト）
            items = await _genie_poll(host, token, space_id, conv_id, msg_id, timeout=60)
            result["poll_ok"] = True
            result["items_count"] = len(items)
            result["items_preview"] = str(items)[:500]
        except Exception as e:
            import traceback
            result["error"] = f"{type(e).__name__}: {e}"
            result["traceback"] = traceback.format_exc()
    return result


@router.post("/chat/stream")
async def mypage_chat_stream(request: MypageChatRequest):
    """Genie API を呼び出し、SSE で回答を返す。"""
    settings = get_settings()
    host = get_databricks_host()
    token = get_oauth_token()
    space_id = settings.sales_mypage_genie_space_id

    async def generate():
        try:
            yield f"data: {json.dumps({'type': 'progress', 'message': 'データを分析中...'})}\n\n"

            if not host or not token or not space_id:
                # デモモード
                await asyncio.sleep(0.5)
                demo_answer = _demo_answer(request.sales_rep_name, request.message)
                chunk_size = 30
                for i in range(0, len(demo_answer), chunk_size):
                    yield f"data: {json.dumps({'type': 'content', 'content': demo_answer[i:i+chunk_size]})}\n\n"
                    await asyncio.sleep(0.02)
                yield "data: [DONE]\n\n"
                return

            contextualized_message = (
                f"私の担当者名は「{request.sales_rep_name}」です。{request.message}"
            )
            conv_id, msg_id = await _genie_start_or_continue(
                host, token, space_id, request.session_id, contextualized_message
            )

            items = await _genie_poll(host, token, space_id, conv_id, msg_id)

            for item in items:
                if item["type"] == "text":
                    content = item["content"]
                    chunk_size = 30
                    for i in range(0, len(content), chunk_size):
                        yield f"data: {json.dumps({'type': 'content', 'content': content[i:i+chunk_size]})}\n\n"
                        await asyncio.sleep(0.02)
                elif item["type"] == "table":
                    yield f"data: {json.dumps({'type': 'table', 'columns': item['columns'], 'rows': item['rows']})}\n\n"

            yield "data: [DONE]\n\n"
        except Exception as e:
            print(f"[mypage/chat] error: {type(e).__name__}: {e}")
            yield f"data: {json.dumps({'type': 'error', 'error': str(e)})}\n\n"
            yield "data: [DONE]\n\n"

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Content-Encoding": "none",
            "X-Accel-Buffering": "no",
        },
    )


def _demo_answer(sales_rep_name: str, message: str) -> str:
    """デモモード用のダミー回答。"""
    msg_lower = message.lower()
    name = sales_rep_name.split()[-1]  # 苗字だけ
    if "失注" in message or "なぜ" in message or "原因" in message:
        return (
            f"**{name}さんの今月の失注分析**\n\n"
            "最も多い失注理由は **予算超過（35%）** です。\n\n"
            "特にミニバン・SUVカテゴリで「提案金額が予算を超えた」という理由が多く見られます。\n\n"
            "**改善アクション:**\n"
            "- 商談の序盤で予算上限を確認し、その範囲内の選択肢を先に提示する\n"
            "- 月々の支払額（ローン試算）を早めに見せると予算感のズレを防ぎやすい"
        )
    if "成約率" in message or "成績" in message or "実績" in message:
        demo = _DEMO_STATS.get(sales_rep_name, {})
        rate = demo.get("current_month", {}).get("contract_rate", "—")
        diff = demo.get("rate_diff_from_last_month")
        trend = f"先月比 **+{diff}%** と上昇傾向" if diff and diff > 0 else f"先月比 **{diff}%**"
        return (
            f"**{name}さんの今月の成約率は {rate}%** です。{trend}です。\n\n"
            "得意なカテゴリでの成約が安定しています。引き続き得意層を中心に商談を進めましょう。"
        )
    return (
        f"{name}さんの実績データを確認しました。\n\n"
        "今月の接客では得意カテゴリを中心に成果が出ています。"
        "具体的な失注理由や車種別の傾向を知りたい場合は、さらに詳しく質問してください。"
    )

