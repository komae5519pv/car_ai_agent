"""Customer-related API endpoints with demo data support."""

import json
import os
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from app.database import db
from app.llm import llm
from app.models import Customer, CustomerInsight, CustomerInteraction, APIResponse
from app.config import get_full_table_name
from app.demo_data import (
    get_all_demo_customers,
    get_demo_customer,
    get_demo_insight,
    get_demo_interaction,
)

router = APIRouter(prefix="/api/customers", tags=["customers"])

# Use demo data when DEMO_MODE is set or database is not configured
USE_DEMO = os.getenv("DEMO_MODE", "true").lower() == "true"


@router.get("", response_model=APIResponse)
async def list_customers(
    limit: int = Query(default=50, le=100),
    offset: int = Query(default=0, ge=0),
    search: Optional[str] = None,
) -> APIResponse:
    """Get list of customers with optional search."""
    try:
        if USE_DEMO:
            # Use demo data
            customers = get_all_demo_customers()

            # Apply search filter
            if search:
                search_lower = search.lower()
                customers = [
                    c for c in customers
                    if search_lower in c["name"].lower()
                    or search_lower in c.get("occupation", "").lower()
                ]

            # Apply pagination
            customers = customers[offset : offset + limit]

            return APIResponse(success=True, data=customers)

        # Use database
        table = get_full_table_name("customers")
        query = f"SELECT * FROM {table}"

        if search:
            query += f" WHERE name LIKE '%{search}%' OR occupation LIKE '%{search}%'"

        query += f" LIMIT {limit} OFFSET {offset}"

        results = await db.execute_query(query)
        customers = [Customer(**row) for row in results]

        return APIResponse(success=True, data=[c.model_dump() for c in customers])
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{customer_id}", response_model=APIResponse)
async def get_customer(customer_id: str) -> APIResponse:
    """Get customer by ID."""
    try:
        if USE_DEMO:
            customer = get_demo_customer(customer_id)
            if not customer:
                raise HTTPException(status_code=404, detail="Customer not found")
            return APIResponse(success=True, data=customer)

        table = get_full_table_name("customers")
        query = f"SELECT * FROM {table} WHERE customer_id = '{customer_id}'"

        results = await db.execute_query(query)

        if not results:
            raise HTTPException(status_code=404, detail="Customer not found")

        customer = Customer(**results[0])
        return APIResponse(success=True, data=customer.model_dump())
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{customer_id}/interaction", response_model=APIResponse)
async def get_customer_interaction(customer_id: str) -> APIResponse:
    """Get customer interaction (conversation transcript)."""
    try:
        if USE_DEMO:
            interaction = get_demo_interaction(customer_id)
            if not interaction:
                raise HTTPException(status_code=404, detail="Interaction not found")
            return APIResponse(success=True, data=interaction)

        table = get_full_table_name("customer_interactions")
        query = f"""
            SELECT * FROM {table}
            WHERE customer_id = '{customer_id}'
            ORDER BY interaction_date DESC
            LIMIT 1
        """

        results = await db.execute_query(query)

        if not results:
            raise HTTPException(status_code=404, detail="Interaction not found")

        return APIResponse(success=True, data=results[0])
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{customer_id}/insights", response_model=APIResponse)
async def get_customer_insights(customer_id: str) -> APIResponse:
    """Get AI-extracted insights for a customer."""
    try:
        if USE_DEMO:
            # Use pre-defined insights from demo data
            insight = get_demo_insight(customer_id)
            if insight:
                return APIResponse(success=True, data=insight)

        # Get customer data
        if USE_DEMO:
            customer = get_demo_customer(customer_id)
            interaction = get_demo_interaction(customer_id)
        else:
            table = get_full_table_name("customers")
            query = f"SELECT * FROM {table} WHERE customer_id = '{customer_id}'"
            results = await db.execute_query(query)

            if not results:
                raise HTTPException(status_code=404, detail="Customer not found")

            customer = results[0]
            interaction = None

        if not customer:
            raise HTTPException(status_code=404, detail="Customer not found")

        # Generate insights using LLM if no pre-defined insight
        transcript_text = ""
        if interaction and interaction.get("transcript"):
            transcript_text = f"\n\n商談録音テキスト:\n{interaction['transcript']}"

        prompt = f"""以下の顧客データを分析し、車両購入に関するインサイトを抽出してください。

顧客情報:
- 名前: {customer['name']}
- 年齢: {customer['age']}歳
- 職業: {customer.get('occupation', '不明')}
- 家族構成: {customer.get('family_structure', '不明')}
- 予算: {customer.get('budget_min', 0):,}円 〜 {customer.get('budget_max', 0):,}円
- 好み: {customer.get('preferences', 'なし')}
{transcript_text}

以下のJSON形式で回答してください:
{{
    "needs": ["ニーズ1", "ニーズ2", "ニーズ3", "ニーズ4"],
    "priorities": ["優先事項1", "優先事項2", "優先事項3"],
    "avoid": ["避けるべき要素1", "避けるべき要素2"],
    "purchase_intent": "購買意欲レベル（高/中/低）と理由の詳細説明",
    "key_insight": "この顧客の深層心理や本当のニーズについての洞察",
    "detected_keywords": ["印象的な発言1", "印象的な発言2"]
}}"""

        messages = [
            {"role": "system", "content": "あなたは自動車販売のエキスパートです。顧客の発言から深層心理を読み取り、的確なインサイトを提供してください。単なる表面的なニーズだけでなく、『なぜそれを求めているのか』という本質を見抜いてください。"},
            {"role": "user", "content": prompt}
        ]

        response = await llm.chat(messages)

        # Parse JSON response
        try:
            # Extract JSON from response (handle markdown code blocks)
            json_str = response
            if "```json" in response:
                json_str = response.split("```json")[1].split("```")[0]
            elif "```" in response:
                json_str = response.split("```")[1].split("```")[0]

            insights_data = json.loads(json_str.strip())
            return APIResponse(success=True, data=insights_data)
        except (json.JSONDecodeError, IndexError):
            # Fallback to default insights
            insights = {
                "needs": ["ファミリー向けの広い車内", "安全装備の充実", "燃費の良さ"],
                "priorities": ["安全性", "居住性", "経済性"],
                "avoid": ["スポーツカー", "2シーター"],
                "purchase_intent": "高（3ヶ月以内に購入予定）",
                "key_insight": "家族のための車選びを重視している",
                "detected_keywords": []
            }
            return APIResponse(success=True, data=insights)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
