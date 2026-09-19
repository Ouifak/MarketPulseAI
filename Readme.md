# 🚀 MarketPulse AI

Pipeline de données de marché en temps réel — de l'ingestion à l'analyse — enrichi d'une couche d'intelligence agentique (LangGraph) et déployé avec une chaîne DevOps complète (Docker, CI/CD, monitoring).

Ce projet ne se contente pas de déplacer de la donnée d'un point A à un point B : il combine **ingénierie de données temps réel**, **système multi-agents pour l'analyse et la prévision**, et **pratiques DevOps de bout en bout** 

---

## 🖼️ Architecture

Finnhub API  -->  Kafka  --> Stockage (Bronze / Silver / Gold)  -->  dbt  -->  Orchestration( Apach airflow) --> ML --> Agentic AI --> Dashboard --> Conteneurisation --> CI / CD --> Observabilité

## 🎯 Pourquoi ce projet

Ce projet part d'un choix assumé : n'utiliser que ce que l'API Finnhub gratuite fournit réellement en temps réel — les **trades** exécutés, via WebSocket. Les endpoints `quote` et `candle` de Finnhub sont soit rate-limités en polling REST, soit verrouillés côté payant. Plutôt que de contourner cette limite avec un abonnement payant, le pipeline **dérive lui-même** les candles (OHLCV) et les métriques de marché (VWAP, volatilité) à partir du flux brut de trades, dans la couche Gold — un exercice d'ingénierie plus intéressant qu'un simple passthrough API → stockage.

---

## 🧱 Stack technique

| Couche | Outils |
|---|---|
| **Ingestion** | Finnhub API (WebSocket, trades uniquement), Kafka (mode KRaft) |
| **Data Lake** | MinIO (S3-compatible) — Bronze → Silver → Gold |
| **Transformation** | dbt, DuckDB (`httpfs`, lecture directe des Parquet sur MinIO) |
| **Orchestration** | Apache Airflow |
| **Intelligence / Forecasting** | LangGraph (agents Supervisor / Insight / Anomaly / Forecast) XGBoost |
| **Interface** | Streamlit |
| **Infrastructure** | Docker, Docker Compose |
| **DevOps** | GitHub Actions (CI/CD), Prometheus + Grafana (monitoring), MLflow (tracking des modèles) |

---

## 🧩 1. Ingestion & Streaming

**Source :** Finnhub API, WebSocket temps réel.

**Une seule donnée ingérée : les trades** (transactions exécutées) — c'est le seul flux réellement temps réel et illimité du plan gratuit.

```json
{"s": "AAPL", "p": 189.23, "v": 100, "t": 1710001234123, "c": ["1"]}
```

**Kafka**
- Topic unique : `stock.trades`
- Clé : le symbole (`s`) — garantit que tous les trades d'un même symbole restent ordonnés dans la même partition
- Valeur : le JSON brut du trade
- Mode **KRaft** (pas de Zookeeper) : le consensus/les métadonnées sont gérés nativement par Kafka via l'algorithme Raft, un seul conteneur au lieu de deux

**Symboles suivis :** `AAPL, MSFT, AMZN, TSLA, NVDA` — large caps très liquides, garantissant un flux continu pendant les heures de marché US.

---

## 🪵 2. Data Lake (MinIO)

Architecture medallion, **trois buckets séparés** : `bronze`, `silver`, `gold`.

### 🟤 Bronze — raw
- Écriture directe des trades reçus de Kafka, sans transformation
- JSON Lines, un fichier par micro-batch : `bronze/date=YYYY-MM-DD/<HHMMSS>-<uuid8>.jsonl`
- Conservé tel quel pour permettre de rejouer tout traitement en cas de bug découvert plus tard

### ⚪ Silver — clean
- Déduplication (sur l'ensemble timestamp + prix + volume, pas seulement le timestamp — plusieurs trades peuvent survenir à la même milliseconde)
- Timestamps convertis en `datetime` UTC
- Typage strict, format **Parquet** (colonnaire, bien plus rapide à requêter que du JSON)

### 🟡 Gold — business
- **Candles (OHLCV)** reconstruits par agrégation des trades sur une fenêtre de temps
- **VWAP**, volatilité, rendement — calculés à partir des trades bruts, pas récupérés d'un endpoint externe

---

## ⚙️ 3. Orchestration (Airflow)

Deux mondes séparés qui tournent en parallèle :
- **Ingestion (WebSocket → Kafka → Bronze)** : processus continu, en dehors d'Airflow
- **Silver → Gold → dbt** : DAG batch, planifié `@hourly`

```
transform_silver >> transform_gold >> dbt_build
```

---

## 🧠 4. Transformation & Modélisation (dbt + DuckDB)

Pas de data warehouse cloud payant — **DuckDB** interroge directement les fichiers Parquet sur MinIO via l'extension `httpfs`.

- `stg_trades` (staging)
- `fact_trades`, `dim_symbol` (modèles de faits/dimensions)
- `agg_stock_metrics` (VWAP, volatilité, rendement)
- Tests dbt : `not_null`, `unique`

---

## 🤖 5. Agents & Intelligence

- **Supervisor** (LangGraph) qui route la requête vers l'agent pertinent
- **Agent Insight** — résume les tendances à partir de Gold
- **Agent Anomaly** — détecte les mouvements de prix/volatilité anormaux
- **Agent Forecast** — interroge les modèles Prophet / SARIMA / XGBoost
- Interface conversationnelle en **Streamlit**

---

## 🐳 6. Infrastructure & DevOps

- **Docker Compose** pour l'environnement complet en local (Kafka, MinIO, Airflow, agents)
- **CI/CD** (GitHub Actions) : lint, tests, build des images à chaque push
- **Monitoring** : Prometheus + Grafana (santé du pipeline, lag Kafka, durée des DAGs)
- **MLflow** : tracking et versioning des modèles de forecasting

---

## 🔄 Flux global

```
Finnhub API (trades)
   ↓ WebSocket
Kafka (stock.trades)
   ↓
MinIO Bronze → Silver → Gold
   ↓
dbt + DuckDB
   ↓
Agents LangGraph (Streamlit)
```

---

## 🚦 Sprints

| Sprint | Contenu 
|---|---|---|
| 0 | Setup Docker (Kafka KRaft + MinIO), structure du repo 
| 1 | Ingestion WebSocket Finnhub → Producer Kafka 
| 2 | Consumer Kafka → Bronze (batching, dédup)
| 3 | Silver → Gold, modélisation dbt
| 4 | Orchestration Airflow 
| 5 | Forecasting ( XGBoost) 
| 6 | Système multi-agents LangGraph + Streamlit 
| 7 | CI/CD, monitoring, MLflow, documentation finale 

---

## ▶️ Démarrage rapide

```bash
git clone https://github.com/Ouifak/MarketPulseAI.git
cd marketpulse-ai

cp .env.example .env
# éditer .env : FINNHUB_API_KEY, identifiants MinIO, symboles suivis

docker compose up -d
docker compose ps   # vérifier que kafka et minio sont "healthy"
```

Console MinIO : `http://localhost:9001`

Lancer l'ingestion (dans un venv Python avec les dépendances installées) :
```bash
python src/ingestion/finnhub_ws_client.py
```