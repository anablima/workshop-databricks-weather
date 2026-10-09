# Workshop: Engenharia de Dados com Databricks & Delta Lake

Workshop prático de engenharia de dados no Databricks usando a API da **OpenWeatherMap** como fonte de dados meteorológicos. O projeto cobre toda a jornada de dados — desde a ingestão de APIs REST até a governança com Unity Catalog — seguindo a **Medallion Architecture** (Bronze → Silver → Gold) com Delta Lake.

## 📌 Visão Geral

O workshop simula um pipeline de dados real: coleta dados de clima atual e previsão de 5 dias para cidades brasileiras, armazena os payloads JSON brutos na camada Bronze, processa e tipifica na Silver, e constrói métricas analíticas na Gold. Inclui desafios de streaming, recursos avançados do Delta Lake e governança com Unity Catalog.

A pasta `desafios/` contém 4 ETLs completos (Bronze → Silver → Gold) que ampliam o workshop com novas fontes de dados: taxas de câmbio (Frankfurter e ExchangeRate-API), dados de países (REST Countries) e indicadores socioeconômicos (World Bank). Cada desafio explora um padrão avançado de engenharia de dados: SCD Tipo 2, normalização de JSONs aninhados e ingestão paginada com controle de estado.

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
│  │ raw_forecast│ │watermark_ctrl │ │_temp         ││
│  │ landing     │ │               │ │temp_mov_avg  ││
│  │             │ │               │ │weather_alerts││
│  └─────────────┘ └───────────────┘ └──────────────┘│
│                                                    │
│  Volume: bronze.landing                            │
│  (JSONs, checkpoints, external tables)             │
└────────────────────────────────────────────────────┘
```

## 📓 Notebooks Principais

Os notebooks devem ser executados **em ordem sequencial**, pois cada um depende dos dados produzidos pelo anterior.

| # | Notebook | Camada | Descrição |
|---|---------|--------|-----------|
| 00 | [00_setup_environment](notebooks/00_setup_environment) | — | Cria o catálogo `workshop_weather`, schemas (`bronze`, `silver`, `gold`), volume `landing` e configura o Secret Scope para a API key |
| 01 | [01_ingest_bronze_current_weather](notebooks/01_ingest_bronze_current_weather) | Bronze | Chama a API `/weather` para **64 cidades** brasileiras (27 capitais + 37 municípios da RMSP) com sufixo `,BR` para desambiguação. Widgets parametrizáveis (`cities`, `catalog`) para uso em Jobs. Grava payloads JSON brutos na Delta Table. Inclui Plano B com `COPY INTO` e Auto Loader |
| 02 | [02_ingest_bronze_forecast](notebooks/02_ingest_bronze_forecast) | Bronze | Chama a API `/forecast` e grava os payloads JSON de previsão de 5 dias na Bronze |
| 03 | [03_process_silver_current_weather](notebooks/03_process_silver_current_weather) | Silver | Lê a Bronze de forma **incremental via `table_changes()` (CDF)** com tabela de watermark, parseia o JSON com `from_json()`, aplica tipagem estrita, deduplicação e `MERGE INTO` idempotente |
| 04 | [04_process_silver_forecast](notebooks/04_process_silver_forecast) | Silver | Explode o array `list[]` em uma linha por slot de 3h, tipifica, aplica DQ e faz `MERGE INTO` |
| 05 | [05_build_gold_metrics](notebooks/05_build_gold_metrics) | Gold | Cria `daily_summary`, `city_ranking_temp` (com `ROW_NUMBER()`), `temperature_moving_avg` (médias móveis 24h/72h com `RANGE BETWEEN INTERVAL`) e a view `weather_alerts`. Aplica `OPTIMIZE` + `ZORDER` |
| 06 | [06_delta_advanced_features](notebooks/06_delta_advanced_features) | — | Time Travel, Change Data Feed (CDF), `OPTIMIZE`/`ZORDER`, `VACUUM`, Spark UI, AQE e Liquid Clustering |
| 06b | [06b_governance_uc](notebooks/06b_governance_uc) | — | `GRANT`/`REVOKE`, hierarquia de permissões do UC, Managed vs External Tables, Row-Level Security e Column Masking |
| 07 | [07_streaming_silver_current](notebooks/07_streaming_silver_current) | Silver | Desafio: substitui o processamento batch do notebook 03 por Structured Streaming com `foreachBatch` + `trigger(availableNow=True)` |

## 🧩 Desafios ETL

A pasta `desafios/` contém 4 pipelines ETL completos (Bronze → Silver → Gold) com fontes de dados independentes. Cada desafio cria seu próprio schema no catálogo `workshop_weather` e pode ser executado de forma autônoma após o notebook `00_setup_environment`.

| # | Notebook | Schema | Descrição |
|---|---------|--------|-----------|
| D1 | [D1_ETL_frankfurter](desafios/D1_ETL_frankfurter) | `exchange` | Pipeline Bronze → Silver → Gold de taxas de câmbio (Frankfurter API). Ingestão batch incremental, Delta append-only na Bronze, `MERGE INTO` por chave composta, window functions e `OPTIMIZE + ZORDER` |
| D2 | [D2_ETL_exchangerate](desafios/D2_ETL_exchangerate) | `exchange` | **SCD Tipo 2** de taxas de câmbio (ExchangeRate-API). `MERGE INTO` com múltiplos `WHEN MATCHED`, lógica de update + insert, controle de vigência (`valid_from`, `valid_to`, `is_current`) |
| D3 | [D3_ETL_restcountries](desafios/D3_ETL_restcountries) | `countries` | Normalização de JSONs aninhados (REST Countries API). `explode()`, `explode(map_entries())`, `MapType`, `ArrayType`, tabelas dimensão em Star Schema |
| D4 | [D4_ETL_worldbank](desafios/D4_ETL_worldbank) | `worldbank` | Ingestão paginada com controle de estado (World Bank API). Tabela de watermark/controle, `explode()` de array JSON, tabela de fato volumosa, Gold com pivot de indicadores |

## 🚀 Como Executar

1. **Pré-requisito:** Obter uma API key gratuita em [openweathermap.org](https://openweathermap.org/api)
2. **Execute o notebook `00_setup_environment`** para criar o catálogo, schemas e volume
3. **Configure o Secret Scope** `openweather` com a key `api_key` (instruções no notebook 00)
4. **Execute os notebooks em sequência** (01 → 02 → 03 → 04 → 05 → 06 → 06b → 07)
5. **Desafios ETL:** execute os notebooks da pasta `desafios/` (D1 → D2 → D3 → D4) após o setup. Cada desafio é independente e cria seu próprio schema
6. **Agendamento (opcional):** o job `workshop-weather-daily-pipeline` executa os notebooks 01 → 03 → 05 diariamente às 12:30 no fuso `America/Sao_Paulo`. Para executar manualmente, use **Run now** na página do Job
7. **Genie Space:** o space "Workshop Weather Analytics" permite fazer perguntas em linguagem natural sobre as tabelas Gold (`temperature_moving_avg`, `daily_summary`, `city_ranking_temp`)

> **Dica:** Cada notebook inclui um checklist no final. Confirme todos os itens antes de avançar para o próximo.

## 🔧 Tecnologias e Conceitos Abordados

* **Unity Catalog** — catálogos, schemas, volumes, GRANT/REVOKE, Row-Level Security, Column Masking
* **Delta Lake** — `MERGE INTO`, `COPY INTO`, Time Travel, Change Data Feed, `OPTIMIZE`/`ZORDER`, `VACUUM`
* **Medallion Architecture** — Bronze (dado bruto), Silver (limpo e tipado), Gold (métricas analíticas)
* **Structured Streaming** — `readStream`/`writeStream`, `foreachBatch`, `trigger(availableNow=True)`, checkpoints
* **PySpark** — `from_json()`, `explode()`, `explode(map_entries())`, window functions, `StructType`, `MapType`, Data Quality flags
* **SCD Tipo 2** — Slowly Changing Dimensions, controle de vigência com `valid_from`/`valid_to`/`is_current`
* **Ingestão Paginada** — controle de estado via tabela de watermark, paginação de APIs REST
* **Star Schema** — tabelas dimensão normalizadas (`dim_country`, `dim_country_language`, `dim_country_currency`)
* **Databricks Secrets** — armazenamento seguro de credenciais com redação em logs
* **Otimização** — Adaptive Query Execution (AQE), Liquid Clustering, Spark UI
* **Change Data Feed (CDF)** — leitura incremental via `table_changes()` com tabela de watermark para controle de versão
* **Window Functions Avançadas** — `RANGE BETWEEN INTERVAL 24/72 HOURS PRECEDING` para médias móveis de temperatura
* **Lakeflow Jobs** — agendamento multi-tarefa (Bronze → Silver → Gold) com retry e dependências
* **Genie Space** — queries em linguagem natural sobre as tabelas Gold do pipeline

## 📂 Estrutura do Repositório

```
workshop-databricks-weather/
├── README.md
├── notebooks/
│   ├── 00_setup_environment
│   ├── 01_ingest_bronze_current_weather
│   ├── 02_ingest_bronze_forecast
│   ├── 03_process_silver_current_weather
│   ├── 04_process_silver_forecast
│   ├── 05_build_gold_metrics
│   ├── 06_delta_advanced_features
│   ├── 06b_governance_uc
│   └── 07_streaming_silver_current
└── desafios/
    ├── D1_ETL_frankfurter
    ├── D2_ETL_exchangerate
    ├── D3_ETL_restcountries
    └── D4_ETL_worldbank
```


---

*Workshop: Engenharia de Dados com Databricks & Delta Lake — OpenWeatherMap API*