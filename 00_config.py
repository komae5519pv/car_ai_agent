# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="background: linear-gradient(135deg, #1B2A4A 0%, #2C3E6B 50%, #1B3139 100%); padding: 20px 30px; border-radius: 10px; margin-bottom: 20px;">
# MAGIC   <div style="display: flex; align-items: center; gap: 15px;">
# MAGIC     <img src="https://www.databricks.com/wp-content/uploads/2022/06/db-nav-logo.svg" width="40" style="filter: brightness(2);"/>
# MAGIC     <div>
# MAGIC       <h1 style="color: #FFFFFF; margin: 0; font-size: 28px;">中古車販売 AI デモ</h1>
# MAGIC       <p style="color: #B0BEC5; margin: 5px 0 0 0; font-size: 16px;">00_config - 共通設定</p>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left:4px solid #607d8b;background:#eceff1;border-radius:8px;padding:16px 20px;margin:16px 0;">
# MAGIC   <div style="display:flex;align-items:flex-start;gap:12px;">
# MAGIC     <span style="font-size:20px;">⚙️</span>
# MAGIC     <div>
# MAGIC       <div style="font-weight:700;font-size:15px;margin-bottom:4px;">設定</div>
# MAGIC       <div style="font-size:14px;color:#333;line-height:1.6;">
# MAGIC         このノートブックは全ハンズオンで共通使用する<strong>カタログ・スキーマ</strong>を設定します。<br/>
# MAGIC         <strong>サーバレスコンピュート</strong>または<strong>SQL Warehouse</strong>で実行してください。
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,Step1: データパイプライン用（01〜04 実行前に設定）
'''Step1: データパイプライン用（01〜04 実行前に設定）'''
# Unity Catalog 設定
catalog_name = "konomi_demo_catalog"        # 任意のカタログ名に変更してください
schema_name = "car_ai_demo"                 # 任意のスキーマ名に変更してください
VOLUME_NAME = "images"                      # 固定：車両画像を格納するボリューム名
RAW_VOLUME_NAME = "raw_data"                # 固定：生データ格納用ボリューム名
KNOWLEDGE_VOLUME_NAME = "knowledge"         # 固定：ナレッジアシスタント用テキスト格納ボリューム名

# LLM 設定
LLM_MODEL = "databricks-claude-sonnet-4"    # Foundation Model API のモデル名

# デモ担当営業（Gold レイヤーの AI 処理対象）
SALES_REP_NAME = "大前 このみ"                # 任意の名前に変更してください（デモで使う営業担当者名）

'''Step2: エージェント＆アプリ用（06〜08 の手順完了後に設定）'''
# Genie Space ID（06_Genie作成手順 で作成後に記入）
GENIE_VEHICLE_ASSISTANT_ID = "01f130f0423e1a6c86735b705148ccfa"   # 車両営業アシスタント Genie
GENIE_MYPAGE_ID = "01f130f1da9a150caecac7a19f5b4317"              # マイページ Genie
GENIE_DASHBOARD_ID = "01f130f284c21b4fb53fd6e6703731d7"           # ダッシュボード Genie

# Agent Bricks（07/08 の手順完了後に記入）
KA_ENDPOINT_NAME = "ka-8f6c4a51-endpoint"             # ナレッジアシスタント エンドポイント名
MAS_ENDPOINT_NAME = "mas-e9f1d14c-endpoint"            # マルチエージェントスーパーバイザー エンドポイント名

# COMMAND ----------

# DBTITLE 1,リセット用（必要な場合のみコメント解除）
# spark.sql(f"DROP SCHEMA IF EXISTS {catalog_name}.{schema_name} CASCADE")

# COMMAND ----------

# DBTITLE 1,カタログ・スキーマ作成
spark.sql(f"""
    CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}
    COMMENT '中古車販売 AI デモ用スキーマ — 車両推薦 AI エージェント'
""")

spark.sql(f"USE CATALOG {catalog_name};")
spark.sql(f"USE SCHEMA {schema_name};")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {VOLUME_NAME} COMMENT '車両画像を格納するボリューム'")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {RAW_VOLUME_NAME} COMMENT '生データ格納用ボリューム'")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {KNOWLEDGE_VOLUME_NAME} COMMENT 'ナレッジアシスタント用テキストファイル格納ボリューム'")

# COMMAND ----------

print("=" * 50)
print("  設定サマリー")
print("=" * 50)
print(f"  Catalog         : {catalog_name}")
print(f"  Schema          : {schema_name}")
print(f"  LLM Model       : {LLM_MODEL}")
print(f"  Sales Rep       : {SALES_REP_NAME}")
print("=" * 50)
print("  セットアップ完了！")
print("=" * 50)
