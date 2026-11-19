# ⚡ Databricks + PokeAPI: Arquitetura Medalhão

Este projeto demonstra a implementação de um pipeline de dados completo utilizando a **PokeAPI** e o ecossistema **Databricks**. O objetivo é aplicar os conceitos da **Arquitetura Medalhão (Medallion)** para transformar dados brutos de uma API em tabelas analíticas refinadas.

---

## 🏗️ 1. Visão Geral da Arquitetura

O pipeline utiliza o **Delta Lake** para gerenciar o armazenamento em três camadas lógicas:

- **🥉 Bronze**: Dados crus em formato JSON, preservando a fidelidade da fonte.
- **🥈 Silver**: Dados limpos e normalizados em tabelas relacionais (Pokémon, Tipos e Stats).
- **🥇 Gold**: Tabelas prontas para análise, com métricas de poder, rankings e tiers (S/A/B/C).

### Organização de Catálogo e Schemas
- **Catálogo**: `pokemons`
- **Schemas**: `bronze`, `silver`, `gold`

---

## 📥 2. Camada Bronze – Ingestão

**Objetivo:** Persistir os dados da PokeAPI sem transformações destrutivas.

- **Método:** O JSON é capturado via `response.text` e armazenado como `STRING`.
- **Metadados:** Inclusão de colunas como `pokemon_id`, `ingestion_date`, `api_status_code` e `source_system`.
- **Falhas de rede:** a requisição que falha vira uma linha com `raw_json` e `api_status_code` nulos; a carga continua.
- **Carga idempotente:** a escrita usa `replaceWhere` na partição `ingestion_date`, então rodar a mesma data de novo substitui a carga em vez de duplicá-la.

| Coluna | Tipo | Descrição |
|---|---|---|
| `pokemon_id` | int | ID do Pokémon |
| `raw_json` | string | JSON completo da API |
| `ingestion_ts` | timestamp | Carimbo de tempo da carga |

---

## ⚙️ 3. Camada Silver – Limpeza e Normalização

A Silver transforma o JSON cru em tabelas estruturadas utilizando `from_json` com um schema pré-definido.

### 📊 Tabelas Principais:
1.  **`silver.pokemon`**: Atributos básicos. A API devolve altura em decímetros e peso em hectogramas; a Silver converte para `height_cm` e `weight_kg`.
2.  **`silver.pokemon_types`**: Relação normalizada Pokémon x Tipo (via `explode`).
3.  **`silver.pokemon_stats`**: Stats base (HP, Attack, etc.) normalizados por linha.

> **Boas Práticas:** Filtramos registros nulos e particionamos as tabelas por `ingestion_date`. Reprocessar uma data (`replaceWhere`) substitui só aquela partição e mantém as outras.

---

## 🏆 4. Camada Gold – Métricas e Rankings

Aqui os dados são transformados em valor de negócio através de **Window Functions** do PySpark. A Gold lê da Silver apenas a data processada (`process_date`).

- **Cálculo de Poder:** `total_power` é a soma de todos os stats base.
- **Tier List:** Divisão dos Pokémon em quartis utilizando `ntile(4)`:
    - **Tier S**: Top 25% (Lendários e elites).
    - **Tier A, B e C**: os quartis seguintes, nessa ordem.
- **Ranking por Tipo:** Média de poder por tipo primário para identificar as classes mais fortes.

---

## 🔍 5. Consultas e Dashboards (Databricks SQL)

Exemplos de consultas que alimentam o dashboard:

```sql
-- Top 5 Pokémon mais fortes
SELECT name_clean, primary_type, total_power, tier_label 
FROM pokemons.gold.pokemon_tier_list 
ORDER BY total_power DESC 
LIMIT 5;

-- Comparativo de força média por tipo
SELECT primary_type, avg_total_power, pokemon_count 
FROM pokemons.gold.type_power_ranking 
ORDER BY avg_total_power DESC;
```

## 6. Orquestração com Workflows

O pipeline é automatizado através de um Databricks Job com três tarefas sequenciais:

- Task 1 (Bronze): Notebook de Ingestão.
- Task 2 (Silver): Notebook de Normalização.
- Task 3 (Gold): Notebook Analítico.

Passe a mesma data para as três tarefas: `ingestion_date` na Bronze e `process_date` na Silver e na Gold (sem parâmetro, todas usam a data do dia).

## 7. Testes

Os testes rodam os três notebooks como estão, numa sessão local de Spark com Delta Lake. Só o que existe apenas no Databricks é simulado: `dbutils.widgets`, `display`, o catálogo `pokemons` e a PokeAPI (respostas fixas, sem rede).

Eles cobrem a falha de rede na Bronze, a carga idempotente, o reprocessamento de uma data na Silver, a conversão de unidades, o tier list com os quatro tiers e a Gold lendo só a data processada.

Requisitos: Python 3.12 e Java 17.

```bash
python3.12 -m venv .venv
.venv/bin/pip install -r requirements-dev.txt
.venv/bin/pytest
```
