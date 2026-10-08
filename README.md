# Workshop: Engenharia de Dados com Databricks & Delta Lake

Workshop prático de engenharia de dados no Databricks usando a API da **OpenWeatherMap** como fonte de dados meteorológicos. O projeto cobre toda a jornada de dados — desde a ingestão de APIs REST até a governança com Unity Catalog — seguindo a **Medallion Architecture** (Bronze → Silver → Gold) com Delta Lake.

## 📌 Visão Geral

O workshop simula um pipeline de dados real: coleta dados de clima atual e previsão de 5 dias para cidades brasileiras, armazena os payloads JSON brutos na camada Bronze, processa e tipifica na Silver, e constrói métricas analíticas na Gold. Inclui desafios de streaming, recursos avançados do Delta Lake e governança com Unity Catalog.

## 🏗️ Arquitetura

```
OpenWeatherMap API
       │
       ▼
┌────────────────────────────────────────────────────┐
│              Unity Catalog                         │
│  Catalog: workshop_weather                         │
│                                                    │
│  ┌─── bronze ──┐ ┌──── silver ───┐ ┌──── gold ────┐│
│  │ raw_current │ │current_weather│ │daily_summary ││
│  │ _weather    │ │forecast       │ │city_ranking  ││
│  │ raw_forecast│ │               │ │_temp         ││
│  │ landing     │ │               │ │weather_alerts││
│  └─────────────┘ └───────────────┘ └──────────────┘│
│                                                    │
│  Volume: bronze.landing                            │
│  (JSONs, checkpoints, external tables)             │
└────────────────────────────────────────────────────┘
```

## 📓 Notebooks

Os notebooks devem ser executados **em ordem sequencial**, pois cada um depende dos dados produzidos pelo anterior.

| # | Notebook | Camada | Descrição |
|---|---------|--------|-----------|
| 00 | [00_setup_environment](notebooks/00_setup_environment) | — | Cria o catálogo `workshop_weather`, schemas (`bronze`, `silver`, `gold`), volume `landing` e configura o Secret Scope para a API key |
| 01 | [01_ingest_bronze_current_weather](notebooks/01_ingest_bronze_current_weather) | Bronze | Chama a API `/weather` para 4 cidades brasileiras e grava os payloads JSON brutos na Delta Table. Inclui Plano B com `COPY INTO` e Auto Loader |
| 02 | [02_ingest_bronze_forecast](notebooks/02_ingest_bronze_forecast) | Bronze | Chama a API `/forecast` e grava os payloads JSON de previsão de 5 dias na Bronze |
| 03 | [03_process_silver_current_weather](notebooks/03_process_silver_current_weather) | Silver | Lê a Bronze, parseia o JSON com `from_json()`, aplica tipagem estrita, regras de Data Quality e `MERGE INTO` idempotente |
| 04 | [04_process_silver_forecast](notebooks/04_process_silver_forecast) | Silver | Explode o array `list[]` em uma linha por slot de 3h, tipifica, aplica DQ e faz `MERGE INTO` |
| 05 | [05_build_gold_metrics](notebooks/05_build_gold_metrics) | Gold | Cria `daily_summary`, `city_ranking_temp` (com `ROW_NUMBER()`) e a view `weather_alerts`. Aplica `OPTIMIZE` + `ZORDER` |
| 06 | [06_delta_advanced_features](notebooks/06_delta_advanced_features) | — | Time Travel, Change Data Feed (CDF), `OPTIMIZE`/`ZORDER`, `VACUUM`, Spark UI, AQE e Liquid Clustering |
| 06b | [06b_governance_uc](notebooks/06b_governance_uc) | — | `GRANT`/`REVOKE`, hierarquia de permissões do UC, Managed vs External Tables, Row-Level Security e Column Masking |
| 07 | [07_streaming_silver_current](notebooks/07_streaming_silver_current) | Silver | Desafio: substitui o processamento batch do notebook 03 por Structured Streaming com `foreachBatch` + `trigger(availableNow=True)` |

## 🚀 Como Executar

1. **Pré-requisito:** Obter uma API key gratuita em [openweathermap.org](https://openweathermap.org/api)
2. **Execute o notebook `00_setup_environment`** para criar o catálogo, schemas e volume
3. **Configure o Secret Scope** `openweather` com a key `api_key` (instruções no notebook 00)
4. **Execute os notebooks em sequência** (01 → 02 → 03 → 04 → 05 → 06 → 06b → 07)

> **Dica:** Cada notebook inclui um checklist no final. Confirme todos os itens antes de avançar para o próximo.

## 🔧 Tecnologias e Conceitos Abordados

* **Unity Catalog** — catálogos, schemas, volumes, GRANT/REVOKE, Row-Level Security, Column Masking
* **Delta Lake** — `MERGE INTO`, `COPY INTO`, Time Travel, Change Data Feed, `OPTIMIZE`/`ZORDER`, `VACUUM`
* **Medallion Architecture** — Bronze (dado bruto), Silver (limpo e tipado), Gold (métricas analíticas)
* **Structured Streaming** — `readStream`/`writeStream`, `foreachBatch`, `trigger(availableNow=True)`, checkpoints
* **PySpark** — `from_json()`, `explode()`, window functions, `StructType`, Data Quality flags
* **Databricks Secrets** — armazenamento seguro de credenciais com redação em logs
* **Otimização** — Adaptive Query Execution (AQE), Liquid Clustering, Spark UI

## 📂 Estrutura do Repositório

```
workshop-databricks-weather/
├── README.md
└── notebooks/
    ├── 00_setup_environment
    ├── 01_ingest_bronze_current_weather
    ├── 02_ingest_bronze_forecast
    ├── 03_process_silver_current_weather
    ├── 04_process_silver_forecast
    ├── 05_build_gold_metrics
    ├── 06_delta_advanced_features
    ├── 06b_governance_uc
    └── 07_streaming_silver_current
```

## 📊 Tabelas Criadas

| Camada | Tabela/View | Descrição |
|--------|-------------|-----------|
| Bronze | `workshop_weather.bronze.raw_current_weather` | Payloads JSON brutos do clima atual |
| Bronze | `workshop_weather.bronze.raw_forecast` | Payloads JSON brutos da previsão de 5 dias |
| Silver | `workshop_weather.silver.current_weather` | Clima atual processado, tipado e com DQ |
| Silver | `workshop_weather.silver.forecast` | Previsão explodida por slot de 3h, tipada e com DQ |
| Gold | `workshop_weather.gold.daily_summary` | Resumo diário por cidade (avg/min/max temp, umidade, vento) |
| Gold | `workshop_weather.gold.city_ranking_temp` | Ranking de cidades por temperatura atual |
| Gold | `workshop_weather.gold.weather_alerts` | View com alertas: HIGH_TEMP, RAIN_RISK, STORM_RISK |

---

*Workshop: Engenharia de Dados com Databricks & Delta Lake — OpenWeatherMap API*