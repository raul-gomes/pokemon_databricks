"""End-to-end checks of the bronze → silver → gold notebooks on local Spark."""

from __future__ import annotations

from collections.abc import Callable

from conftest import FAILING_ID, FAKE_API
from pyspark.sql import SparkSession

BRONZE = "01_bronze_pokemon.ipynb"
SILVER = "02_silver_pokemon.ipynb"
GOLD = "03_gold_pokemon.ipynb"

DAY_ONE = "2026-03-19"
DAY_TWO = "2026-03-20"
LIMIT_WITH_FAILURE = str(FAILING_ID)  # ids 1..4 succeed, id 5 fails
LIMIT_OK = str(len(FAKE_API))

Runner = Callable[..., None]


def run_pipeline(run_notebook: Runner, day: str, limit: str = LIMIT_OK) -> None:
    """Runs the three layers for one date, as the Databricks Job does."""
    run_notebook(BRONZE, pokemon_limit=limit, ingestion_date=day)
    run_notebook(SILVER, process_date=day)
    run_notebook(GOLD, process_date=day)


def count(spark: SparkSession, table: str) -> int:
    return spark.table(table).count()


def test_bronze_records_a_failed_request_without_crashing(
    spark: SparkSession, run_notebook: Runner
) -> None:
    run_notebook(BRONZE, pokemon_limit=LIMIT_WITH_FAILURE, ingestion_date=DAY_ONE)

    failed = (
        spark.table("bronze.pokemons_raw")
        .filter(f"pokemon_id = {FAILING_ID}")
        .collect()
    )
    assert len(failed) == 1
    assert failed[0]["raw_json"] is None
    assert failed[0]["api_status_code"] is None


def test_rerunning_bronze_for_the_same_date_does_not_duplicate(
    spark: SparkSession, run_notebook: Runner
) -> None:
    run_notebook(BRONZE, pokemon_limit=LIMIT_OK, ingestion_date=DAY_ONE)
    run_notebook(BRONZE, pokemon_limit=LIMIT_OK, ingestion_date=DAY_ONE)

    assert count(spark, "bronze.pokemons_raw") == len(FAKE_API)


def test_silver_keeps_other_dates_when_reprocessing_one(
    spark: SparkSession, run_notebook: Runner
) -> None:
    run_pipeline(run_notebook, DAY_ONE)
    run_pipeline(run_notebook, DAY_TWO)
    run_notebook(SILVER, process_date=DAY_ONE)

    dates = {
        row["ingestion_date"]
        for row in spark.table("silver.pokemon")
        .select("ingestion_date")
        .distinct()
        .collect()
    }
    assert dates == {DAY_ONE, DAY_TWO}
    assert count(spark, "silver.pokemon") == 2 * len(FAKE_API)


def test_silver_converts_api_units_to_cm_and_kg(
    spark: SparkSession, run_notebook: Runner
) -> None:
    run_pipeline(run_notebook, DAY_ONE)

    bulbasaur = spark.table("silver.pokemon").filter("pokemon_id = 1").first()
    assert bulbasaur["name_clean"] == "bulbasaur"
    assert bulbasaur["height_cm"] == 70  # 7 dm
    assert bulbasaur["weight_kg"] == 6.9  # 69 hg


def test_silver_skips_requests_that_returned_no_pokemon(
    spark: SparkSession, run_notebook: Runner
) -> None:
    run_notebook(BRONZE, pokemon_limit=LIMIT_WITH_FAILURE, ingestion_date=DAY_ONE)
    run_notebook(SILVER, process_date=DAY_ONE)

    assert count(spark, "silver.pokemon") == len(FAKE_API)
    assert count(spark, "silver.types") == sum(
        len(p["types"]) for p in FAKE_API.values()
    )
    assert count(spark, "silver.pokemon_stats") == sum(
        len(p["stats"]) for p in FAKE_API.values()
    )


def test_gold_tier_list_uses_all_four_tiers(
    spark: SparkSession, run_notebook: Runner
) -> None:
    run_pipeline(run_notebook, DAY_ONE)

    tiers = {
        row["name"]: row["tier_label"]
        for row in spark.table("gold.pokemon_tier_list").collect()
    }
    assert tiers == {
        "venusaur": "S",
        "ivysaur": "A",
        "bulbasaur": "B",
        "charmander": "C",
    }


def test_gold_reads_only_the_processed_date(
    spark: SparkSession, run_notebook: Runner
) -> None:
    run_pipeline(run_notebook, DAY_ONE)
    run_pipeline(run_notebook, DAY_TWO)

    assert count(spark, "gold.pokemon_overview") == len(FAKE_API)
    venusaur = spark.table("gold.pokemon_overview").filter("name = 'venusaur'").first()
    assert venusaur["total_power"] == 80 * 6
    assert venusaur["primary_type"] == "grass"
    assert venusaur["weight_kg"] == 6.9  # not truncated to an integer
