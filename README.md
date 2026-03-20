# Databricks + PokeAPI: Arquitetura Medalhão

Este projeto apresenta um pipeline de dados completo construído no **Databricks**, utilizando a **PokeAPI** como fonte de dados. A estrutura segue a **Arquitetura Medalhão (Medallion)**, transformando dados crus em insights analíticos prontos para dashboards.

---

## 1. Visão Geral da Arquitetura

O fluxo consiste na ingestão de dados via API e processamento em três camadas distintas dentro do **Delta Lake**:

* **Bronze**: Dados crus (Raw) em formato JSON.
* **Silver**: Dados limpos, tipados e normalizados em tabelas relacionais.
* **Gold**: Tabelas agregadas com métricas de negócio, rankings e tier lists.

### Organização de Dados
* **Catálogo:** `pokemons`
* **Schemas:** `bronze`, `silver`, `gold`

---

## 2. Camada Bronze (Ingestão)

### Objetivo
Atuar como a "Single Source of Truth". Armazenamos o JSON original para garantir que qualquer mudança de regra de negócio permita o reprocessamento total dos dados.

**Estrutura da Tabela `bronze.pokemon_raw`:**

| Coluna | Tipo | Descrição |
| :--- | :--- | :--- |
| `pokemon_id` | INT | Identificador único do Pokémon |
| `raw_json` | STRING | JSON completo da PokeAPI (`response.text`) |
| `ingestion_date` | DATE | Data da carga (Partição) |
| `ingestion_ts` | TIMESTAMP | Timestamp exato da ingestão |
| `api_status_code`| INT | Código de retorno da requisição HTTP |

---

## 3. Camada Silver (Normalização)

Nesta etapa, utilizamos o comando `from_json` do PySpark com um schema pré-definido para estruturar os dados.

### Principais Tabelas
1.  **`silver.pokemon`**: Atributos principais como altura, peso e experiência base.
2.  **`silver.pokemon_types`**: Tabela normalizada (N:1) criada através da função `explode()`, permitindo múltiplos tipos por Pokémon.
3.  **`silver.pokemon_stats`**: Valores base de HP, Attack, Defense, Special-Attack, Special-Defense e Speed.

**Exemplo de Normalização de Tipos:**
| pokemon_id | type_slot | type_name |
| :--- | :--- | :--- |
| 1 | 1 | grass |
| 1 | 2 | poison |
| 6 | 1 | fire |

---

## 4. Camada Gold (Business & Analytics)

A camada Gold é onde a mágica acontece para o usuário final. Criamos métricas complexas e classificações.

### Métricas e Rankings
* **Total Power**: Soma de todos os stats base ($HP + ATK + DEF + SP.ATK + SP.DEF + SPEED$).
* **Tier List**: Aplicação de **Window Functions** (`ntile(4)`) para classificar os Pokémon em categorias de força:
    * **Tier S**: Top 25% mais fortes.
    * **Tier A/B**: Intermediários.
    * **Tier C**: Bottom 25% (Pokémon iniciais ou com stats reduzidos).

---

## 📊 5. Consultas e Dashboards

As tabelas Gold são consumidas via **Databricks SQL**. Exemplo de query para o Top 5:

```sql
SELECT 
    name_clean AS name, 
    primary_type, 
    total_power, 
    tier_label
FROM pokemons.gold.pokemon_tier_list
ORDER BY total_power DESC
LIMIT 5;
```
