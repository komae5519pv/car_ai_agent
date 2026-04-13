"""Vehicle recommendation API endpoints with demo data support."""

import json
import os
from typing import Optional

from fastapi import APIRouter, HTTPException

from app.database import db
from app.llm import llm
from app.models import (
    Vehicle,
    VehicleRecommendation,
    RecommendationResponse,
    APIResponse,
)
from app.config import get_full_table_name
from app.demo_data import (
    get_demo_customer,
    get_demo_recommendations,
    get_demo_talk_script,
    get_demo_vehicles_for_customer,
    get_demo_interaction,
    DEMO_VEHICLES,
)

router = APIRouter(prefix="/api", tags=["recommendations"])

# Use demo data when DEMO_MODE is set or database is not configured
USE_DEMO = os.getenv("DEMO_MODE", "true").lower() == "true"


@router.get("/vehicles", response_model=APIResponse)
async def list_vehicles(
    limit: int = 20,
    offset: int = 0,
    body_type: Optional[str] = None,
    min_price: Optional[int] = None,
    max_price: Optional[int] = None,
) -> APIResponse:
    """Get list of available vehicles."""
    try:
        if USE_DEMO:
            vehicles = DEMO_VEHICLES.copy()

            # Apply filters
            if body_type:
                vehicles = [v for v in vehicles if v["body_type"] == body_type]
            if min_price:
                vehicles = [v for v in vehicles if v["price"] >= min_price]
            if max_price:
                vehicles = [v for v in vehicles if v["price"] <= max_price]

            # Apply pagination
            vehicles = vehicles[offset : offset + limit]

            return APIResponse(success=True, data=vehicles)

        # Use database
        table = get_full_table_name("vehicle_inventory")
        conditions = []

        if body_type:
            conditions.append(f"body_type = '{body_type}'")
        if min_price:
            conditions.append(f"price >= {min_price}")
        if max_price:
            conditions.append(f"price <= {max_price}")

        where_clause = " AND ".join(conditions) if conditions else "1=1"
        query = f"""
            SELECT * FROM {table}
            WHERE {where_clause}
            LIMIT {limit} OFFSET {offset}
        """

        results = await db.execute_query(query)
        for v in results:
            if 'image_path' in v and 'image_url' not in v:
                v['image_url'] = v['image_path']
        return APIResponse(success=True, data=results)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/customers/{customer_id}/recommendations", response_model=APIResponse)
async def get_recommendations(customer_id: str) -> APIResponse:
    """Get vehicle recommendations - reads from UC cache, falls back to demo data."""
    try:
        if USE_DEMO:
            return _build_demo_recommendations(customer_id)

        # Read pre-generated recommendations from UC
        rec_table = get_full_table_name("recommendations")
        cached = await db.execute_query(
            f"SELECT * FROM {rec_table} WHERE customer_id = '{customer_id}' ORDER BY generated_at DESC LIMIT 1"
        )
        if cached:
            row = cached[0]
            recs_raw = json.loads(row["recommendations_json"])
            # Normalize image_path → image_url for each vehicle
            for rec in recs_raw:
                v = rec.get("vehicle", {})
                if "image_path" in v and "image_url" not in v:
                    v["image_url"] = v["image_path"]
            return APIResponse(success=True, data={
                "customer_id": customer_id,
                "recommendations": recs_raw,
                "talk_script": row.get("talk_script", ""),
            })

        # No cached data → return error (use 再生成 button to generate)
        raise HTTPException(status_code=404, detail="推薦データが見つかりません。再生成ボタンを押してください。")

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def _build_demo_recommendations(customer_id: str) -> APIResponse:
    """Build response from demo data."""
    recommendations = get_demo_recommendations(customer_id)
    talk_script = get_demo_talk_script(customer_id)
    if recommendations:
        vehicle_map = {v['vehicle_id']: v for v in DEMO_VEHICLES}
        enriched = [
            {"vehicle": vehicle_map[r['vehicle_id']], "match_score": r['match_score'], "reason": r['reason']}
            for r in recommendations if r['vehicle_id'] in vehicle_map
        ]
        return APIResponse(success=True, data={"customer_id": customer_id, "recommendations": enriched, "talk_script": talk_script})
    raise HTTPException(status_code=404, detail="Customer not found")


async def _generate_talk_script(
    customer: dict,
    recommendations: list[dict],
    interaction: Optional[dict] = None
) -> str:
    """Generate talk script for the recommendations (Format B: car-by-car)."""
    vehicles_ranked = []
    for i, rec in enumerate(recommendations[:3], 1):
        v = rec['vehicle']
        price = v.get('price', 0)
        try:
            price_str = f"{int(price):,}万円" if int(price) >= 10000 else f"{price}円"
        except (ValueError, TypeError):
            price_str = str(price)
        vehicles_ranked.append(
            f"第{i}位: {v.get('make','')} {v.get('model','')}（{price_str}）\n"
            f"  推薦理由: {rec.get('reason','')}"
        )
    vehicles_info = "\n".join(vehicles_ranked)

    transcript_context = ""
    if interaction and interaction.get("transcript"):
        transcript_context = f"\n商談録音テキスト（参考）:\n{interaction['transcript'][:2000]}\n"

    key_quotes_context = ""
    if interaction and interaction.get("key_quotes"):
        quotes = "、".join([f'「{q}」' for q in interaction["key_quotes"]])
        key_quotes_context = f"\n顧客の印象的な発言: {quotes}\n"

    prompt = f"""あなたは経験豊富な自動車営業担当者です。以下の顧客に向けた営業トークスクリプトを作成してください。

顧客情報:
- 名前: {customer['name']}（{customer['age']}歳・{customer.get('occupation', '')}）
- 家族構成: {customer.get('family_structure', '')}
- 現在の車: {customer.get('current_vehicle', '')}
- 予算: {customer.get('budget_min', 0):,}〜{customer.get('budget_max', 0):,}円
- 重視点: {customer.get('preferences', '')}
- 背景: {customer.get('background', '')}
{key_quotes_context}{transcript_context}
推薦車両（ランク順）:
{vehicles_info}

【出力フォーマット】必ず以下の見出し構成で出力してください:

## {customer['name']}様へのご提案トーク

### 導入
（この顧客の状況・背景に踏み込んだ自然な一言。商談での発言や具体的な生活シーンに触れ、「この人は自分のことをわかってくれている」と感じさせる。1〜3文程度。）

### 第1位：[車名]（[価格]）── 一番のおすすめです
- **[推薦ポイント1]**：この顧客固有の事情・ニーズに直結した理由。例えば〜〜という場面でこの機能が効く、という形で具体的な生活シーンを自然に盛り込む
- **[推薦ポイント2]**：具体的な数字や使用シーンを交えた理由
- **[推薦ポイント3]**：他との差別化ポイント
*営業としての一言：なぜこの人にこの車が最も合うか、自分の意見を一言*

### 第2位：[車名]（[価格]）
- **[推薦ポイント1]**：この顧客固有の事情に紐付けた理由。例えば〜〜という場面を自然に盛り込む
- **[推薦ポイント2]**：具体的な理由
*（第1位と比較した際の位置づけを一言）*

### 第3位：[車名]（[価格]）
- **[推薦ポイント1]**：この顧客固有の事情に紐付けた理由。例えば〜〜という場面を自然に盛り込む
- **[推薦ポイント2]**：具体的な理由
*（第1位・第2位と比較した際の位置づけを一言）*

### クロージング
（具体的な次のアクションへの自然な誘導。試乗・実車確認など。1〜2文。）

【重要な注意点】
- 導入は「先日はありがとうございました」だけで終わらせず、この顧客の具体的な状況（商談での発言・生活背景）に必ず言及する
- 各推薦ポイントは「この人だからこそ」の理由にする。汎用的なスペック説明にならないこと
- 機能・特徴の説明には「例えば〜〜」の形で顧客の実際の生活シーンを自然に織り込む（「〜〜をご想像ください」「〜〜をイメージしてみてください」などの押しつけ表現は使わない）
- 第1位には営業担当者自身の意見・判断を入れる（「私が一番おすすめする理由は〜」）
- 顧客の呼び方は必ず「苗字＋様」（例：渡辺様、山田様、佐藤様）。下の名前では絶対に呼ばない
- 文体は自然な日本語で、押しつけがましくなく、でも確信を持って伝える口調
- Markdown形式で出力"""

    messages = [
        {"role": "system", "content": "あなたは顧客の深層ニーズを理解し、シャープに刺さる提案ができる優秀な自動車営業担当者です。"},
        {"role": "user", "content": prompt}
    ]

    return await llm.chat(messages)


@router.post("/customers/{customer_id}/recommendations/regenerate", response_model=APIResponse)
async def regenerate_recommendations(customer_id: str) -> APIResponse:
    """Generate new recommendations via LLM (does NOT auto-save)."""
    try:
        if USE_DEMO:
            return _build_demo_recommendations(customer_id)

        # Get customer data
        customers_table = get_full_table_name("customers")
        customer_results = await db.execute_query(
            f"SELECT * FROM {customers_table} WHERE customer_id = '{customer_id}'"
        )
        if not customer_results:
            raise HTTPException(status_code=404, detail="Customer not found")
        customer = customer_results[0]

        # Get vehicles within budget
        vehicles_table = get_full_table_name("vehicle_inventory")
        budget_min = customer.get("budget_min") or 0
        budget_max = customer.get("budget_max") or 99999999
        vehicle_results = await db.execute_query(
            f"SELECT * FROM {vehicles_table} WHERE price BETWEEN {budget_min} AND {budget_max} LIMIT 20"
        )
        if not vehicle_results:
            vehicle_results = await db.execute_query(f"SELECT * FROM {vehicles_table} LIMIT 20")

        for v in vehicle_results:
            if "image_path" in v and "image_url" not in v:
                v["image_url"] = v["image_path"]

        # Get interaction for context
        interactions_table = get_full_table_name("customer_interactions")
        interaction_results = await db.execute_query(
            f"SELECT * FROM {interactions_table} WHERE customer_id = '{customer_id}' ORDER BY interaction_date DESC LIMIT 1"
        )
        interaction = interaction_results[0] if interaction_results else None

        vehicles_info = "\n".join([
            f"- {v['vehicle_id']}: {v['make']} {v['model']} ({v['year']}年) {v['price']:,}円 {v['body_type']} {v['fuel_type']} 特徴:{v.get('features','')}"
            for v in vehicle_results
        ])
        transcript_context = f"\n\n商談録音:\n{interaction['transcript']}" if interaction and interaction.get("transcript") else ""

        prompt = f"""以下の顧客に最適な車両を3台推薦してください。

顧客: {customer['name']}（{customer['age']}歳・{customer.get('occupation','')}）
家族: {customer.get('family_structure','')}
予算: {budget_min:,}〜{budget_max:,}円
重視: {customer.get('preferences','')}
{transcript_context}

利用可能な車両:
{vehicles_info}

JSON形式のみで回答:
{{"recommendations": [{{"vehicle_id": "Vxxx", "match_score": 95, "reason": "推薦理由2〜3文"}}, {{"vehicle_id": "Vxxx", "match_score": 88, "reason": "..."}}, {{"vehicle_id": "Vxxx", "match_score": 82, "reason": "..."}}]}}"""

        messages = [
            {"role": "system", "content": "あなたは自動車販売のエキスパートです。顧客の状況に合わせた説得力ある推薦理由を作成してください。"},
            {"role": "user", "content": prompt}
        ]
        response = await llm.chat(messages)

        # Parse
        json_str = response
        if "```json" in json_str:
            json_str = json_str.split("```json")[1].split("```")[0]
        elif "```" in json_str:
            json_str = json_str.split("```")[1].split("```")[0]
        start = json_str.find('{')
        end = json_str.rfind('}') + 1
        rec_data = json.loads(json_str[start:end].strip())

        vehicle_map = {v['vehicle_id']: v for v in vehicle_results}
        recommendations = [
            {"vehicle": vehicle_map[r["vehicle_id"]], "match_score": r["match_score"], "reason": r["reason"]}
            for r in rec_data.get("recommendations", [])[:3]
            if r["vehicle_id"] in vehicle_map
        ]

        talk_script = await _generate_talk_script(customer, recommendations, interaction)

        return APIResponse(success=True, data={
            "customer_id": customer_id,
            "recommendations": recommendations,
            "talk_script": talk_script,
        })
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/customers/{customer_id}/recommendations/save", response_model=APIResponse)
async def save_recommendations(customer_id: str, body: dict) -> APIResponse:
    """Save generated recommendations to UC recommendations table."""
    try:
        if USE_DEMO:
            return APIResponse(success=True, data={"message": "Demo mode: save skipped"})

        import uuid as _uuid
        recs = body.get("recommendations", [])
        talk_script = body.get("talk_script", "")
        rec_id = str(_uuid.uuid4())
        rec_json = json.dumps(recs, ensure_ascii=False).replace("'", "''")
        talk_escaped = talk_script.replace("'", "''")

        rec_table = get_full_table_name("recommendations")
        await db.execute_query(
            f"DELETE FROM {rec_table} WHERE customer_id = '{customer_id}'"
        )
        await db.execute_query(
            f"""INSERT INTO {rec_table}
            (recommendation_id, customer_id, recommendations_json, talk_script, generated_at, model_used)
            VALUES ('{rec_id}', '{customer_id}', '{rec_json}', '{talk_escaped}', current_timestamp(), 'databricks-claude-sonnet-4')"""
        )
        return APIResponse(success=True, data={"message": "保存しました"})
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
