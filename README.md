# Car AI Demo - 車両提案AIアプリケーション

現場営業担当者向けAI車両レコメンデーションシステム。顧客のニーズを分析し、最適な車両を提案します。

## 機能

### 現場営業向けUI (`/sales`)
- **顧客一覧・選択**: 顧客リストの表示と検索
- **顧客インサイト**: AIによる顧客ニーズ分析
- **車両レコメンド**: 3台の推薦車両（マッチ度スコア付き）
- **トークスクリプト**: 顧客特性に合わせた提案トーク
- **Ask AI チャット**: 常駐サイドバーでの自由質問

### 管理者向けUI (`/admin`)
- **ダッシュボード**: 主要KPIの表示
- **AI推論ログ**: MLflow Tracing連携
- **AI Gateway監視**: エンドポイントモニタリング
- **評価管理**: AI出力の人間評価とGround Truth登録
- **データカタログ**: Unity Catalogテーブル一覧

## 技術スタック

### Frontend
- React 19
- TypeScript 5.9
- TailwindCSS 4
- Zustand (状態管理)
- React Router 7
- React Icons

### Backend
- FastAPI
- Python 3.11+
- Databricks SQL Connector
- OpenAI SDK (Foundation Model API)
- MLflow Tracing

### Infrastructure
- Databricks Apps
- Unity Catalog
- Foundation Model API (Claude Sonnet 4)

## セットアップ

### 前提条件
- Node.js 20+
- Python 3.11+
- Databricks CLI 0.229.0+
- uv (Python package manager)

### ローカル開発

1. **リポジトリのクローン**
```bash
cd ~/code/car-ai-demo
```

2. **バックエンドのセットアップ**
```bash
cd backend
uv venv
source .venv/bin/activate
uv pip install -r requirements.txt
cp .env.example .env
# .envファイルを編集してDatabricks設定を入力
```

3. **フロントエンドのセットアップ**
```bash
cd frontend
npm install
```

4. **開発サーバーの起動**

ターミナル1 (Backend):
```bash
cd backend
source .venv/bin/activate
python run.py
```

ターミナル2 (Frontend):
```bash
cd frontend
npm run dev
```

5. **ブラウザでアクセス**
```
http://localhost:5173
```

## Databricks Appsへのデプロイ

### 1. フロントエンドのビルド
```bash
cd frontend
npm run build
```

### 2. Databricks認証
```bash
databricks auth login --host https://e2-demo-field-eng.cloud.databricks.com
```

### 3. ワークスペースへの同期
```bash
databricks sync . /Workspace/Users/your-email/car-ai-demo --watch
```

### 4. アプリの作成とデプロイ
```bash
# アプリの作成（初回のみ）
databricks apps create car-ai-demo --description "Car AI Demo - 車両提案AIアプリ"

# デプロイ
databricks apps deploy car-ai-demo --source-code-path /Workspace/Users/your-email/car-ai-demo
```

### 5. リソースの追加（UI経由）
1. Compute > Apps > car-ai-demo > Edit
2. "+ Add resource" をクリック
3. SQL Warehouse を追加（key: `sql-warehouse`）
4. 保存して再デプロイ

## プロジェクト構造

```
car-ai-demo/
├── README.md               # このファイル
├── app.yaml                # Databricks Apps設定
├── .gitignore
├── backend/
│   ├── pyproject.toml
│   ├── requirements.txt
│   ├── run.py              # エントリーポイント
│   └── app/
│       ├── __init__.py
│       ├── main.py         # FastAPIアプリケーション
│       ├── config.py       # 設定管理
│       ├── database.py     # DBコネクション
│       ├── llm.py          # LLMクライアント
│       ├── models.py       # Pydanticモデル
│       └── routers/
│           ├── __init__.py
│           ├── customers.py
│           ├── recommendations.py
│           ├── chat.py
│           └── admin.py
└── frontend/
    ├── package.json
    ├── tsconfig.json
    ├── vite.config.ts
    ├── index.html
    └── src/
        ├── main.tsx
        ├── App.tsx
        ├── index.css
        ├── api/
        │   └── index.ts
        ├── store/
        │   └── index.ts
        ├── types/
        │   └── index.ts
        ├── layouts/
        │   ├── SalesLayout.tsx
        │   └── AdminLayout.tsx
        ├── components/
        │   ├── common/
        │   │   ├── Button.tsx
        │   │   ├── Card.tsx
        │   │   ├── Badge.tsx
        │   │   ├── LoadingSpinner.tsx
        │   │   └── MatchScore.tsx
        │   └── chat/
        │       └── ChatSidebar.tsx
        └── pages/
            ├── sales/
            │   ├── CustomerList.tsx
            │   └── CustomerDetail.tsx
            └── admin/
                ├── Dashboard.tsx
                ├── TraceLogs.tsx
                ├── GatewayMonitor.tsx
                ├── Evaluations.tsx
                └── DataCatalog.tsx
```

## Databricks設定

| 項目 | 値 |
|------|-----|
| Workspace | e2-demo-field-eng.cloud.databricks.com |
| Profile | DEFAULT |
| Catalog | komae_demo_v4 |
| Schema | car_ai_demo |
| LLM Model | databricks-claude-sonnet-4 |

## API エンドポイント

### 顧客関連
- `GET /api/customers` - 顧客一覧
- `GET /api/customers/:id` - 顧客詳細
- `GET /api/customers/:id/insights` - AIインサイト

### 車両レコメンド
- `GET /api/customers/:id/recommendations` - レコメンド取得
- `POST /api/recommendations/regenerate` - 再生成

### チャット
- `POST /api/chat` - チャット（通常）
- `POST /api/chat/stream` - ストリーミング
- `GET /api/chat/history/:session` - 履歴取得

### 管理者向け
- `GET /api/admin/stats` - ダッシュボード統計
- `GET /api/admin/traces` - MLflowトレース一覧
- `GET /api/admin/gateway/metrics` - Gatewayメトリクス
- `GET /api/admin/catalog/tables` - UCテーブル一覧
- `POST /api/admin/evaluations` - 評価登録

## ライセンス

Demo Use Only
