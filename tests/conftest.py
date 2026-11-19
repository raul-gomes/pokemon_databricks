"""Runs the Databricks notebooks on a local Spark + Delta session.

The notebooks are executed cell by cell as they are in the repo. Only what
exists solely on Databricks is replaced: `dbutils.widgets`, `display`, the
`pokemons` Unity Catalog (mapped to the local `spark_catalog`) and the PokeAPI
(a fake `requests.get` serving fixed payloads, so tests never touch the network).
"""

from __future__ import annotations

import json
import os
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
import requests
from delta import configure_spark_with_delta_pip
from pyspark.sql import DataFrame, SparkSession

REPO_ROOT = Path(__file__).resolve().parent.parent
LOCAL_CATALOG = "spark_catalog."
UNITY_CATALOG = "pokemons."
SCHEMAS = ("bronze", "silver", "gold")

STAT_NAMES = ("hp", "attack", "defense", "special-attack", "special-defense", "speed")


def pokemon_payload(
    pokemon_id: int, name: str, base_stat: int, types: tuple[str, ...]
) -> dict[str, Any]:
    """Builds a minimal PokeAPI `/pokemon/{id}` payload with every stat set to `base_stat`."""
    return {
        "id": pokemon_id,
        "name": name.upper(),
        "height": 7,  # decimetres, as the API returns it
        "weight": 69,  # hectograms, as the API returns it
        "base_experience": 64,
        "is_default": True,
        "types": [
            {"slot": slot, "type": {"name": type_name, "url": ""}}
            for slot, type_name in enumerate(types, start=1)
        ],
        "stats": [
            {"base_stat": base_stat, "effort": 0, "stat": {"name": stat, "url": ""}}
            for stat in STAT_NAMES
        ],
    }


# Four Pokémon with distinct power, so ntile(4) puts exactly one in each tier.
FAKE_API: dict[int, dict[str, Any]] = {
    1: pokemon_payload(1, "Bulbasaur", 50, ("grass", "poison")),
    2: pokemon_payload(2, "Ivysaur", 60, ("grass", "poison")),
    3: pokemon_payload(3, "Venusaur", 80, ("grass",)),
    4: pokemon_payload(4, "Charmander", 40, ("fire",)),
}
FAILING_ID = 5  # requests to this id raise a connection error


class FakeResponse:
    """The subset of `requests.Response` the bronze notebook reads."""

    def __init__(self, status_code: int, text: str) -> None:
        self.status_code = status_code
        self.text = text


def fake_get(url: str, timeout: float) -> FakeResponse:
    """Serves FAKE_API by id; FAILING_ID raises, unknown ids answer 404 like the real API."""
    pokemon_id = int(url.rstrip("/").rsplit("/", 1)[-1])
    if pokemon_id == FAILING_ID:
        raise requests.ConnectionError("simulated network failure")
    payload = FAKE_API.get(pokemon_id)
    if payload is None:
        return FakeResponse(404, "Not Found")
    return FakeResponse(200, json.dumps(payload))


class FakeWidgets:
    """In-memory stand-in for `dbutils.widgets`; `overrides` play the role of Job parameters."""

    def __init__(self, overrides: dict[str, str]) -> None:
        self._overrides = overrides
        self._defaults: dict[str, str] = {}

    def removeAll(self) -> None:
        self._defaults.clear()

    def text(self, name: str, default: str, label: str) -> None:
        self._defaults[name] = default

    def get(self, name: str) -> str:
        return self._overrides.get(name, self._defaults[name])


class FakeDbutils:
    """`dbutils` with only the widgets the notebooks use."""

    def __init__(self, overrides: dict[str, str]) -> None:
        self.widgets = FakeWidgets(overrides)


def notebook_cells(notebook: str) -> list[str]:
    """Returns the code cells of a notebook, with the Unity Catalog name mapped to the local catalog."""
    content = json.loads((REPO_ROOT / notebook).read_text(encoding="utf-8"))
    return [
        "".join(cell["source"]).replace(UNITY_CATALOG, LOCAL_CATALOG)
        for cell in content["cells"]
        if cell["cell_type"] == "code"
    ]


@pytest.fixture(scope="session")
def spark(tmp_path_factory: pytest.TempPathFactory) -> SparkSession:
    """Local Spark session with Delta Lake and one database per medallion layer."""
    os.environ["PYSPARK_PYTHON"] = (
        sys.executable
    )  # workers must match the driver's Python
    warehouse = tmp_path_factory.mktemp("warehouse")
    builder = (
        SparkSession.builder.master("local[1]")
        .appName("pipeline-tests")
        .config("spark.sql.warehouse.dir", str(warehouse))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(autouse=True)
def clean_catalog(spark: SparkSession) -> None:
    """Gives every test empty bronze/silver/gold databases."""
    for schema in SCHEMAS:
        spark.sql(f"DROP DATABASE IF EXISTS {schema} CASCADE")
        spark.sql(f"CREATE DATABASE {schema}")


@pytest.fixture
def run_notebook(
    spark: SparkSession, monkeypatch: pytest.MonkeyPatch
) -> Callable[..., None]:
    """Executes a notebook's code cells in order, with widget values given as keyword arguments."""
    monkeypatch.setattr(requests, "get", fake_get)
    monkeypatch.setattr("time.sleep", lambda seconds: None)
    # DataFrame.display() exists only on Databricks.
    monkeypatch.setattr(DataFrame, "display", lambda frame: None, raising=False)

    def run(notebook: str, **widgets: str) -> None:
        namespace: dict[str, Any] = {
            "spark": spark,
            "dbutils": FakeDbutils(widgets),
            "display": lambda frame: None,
        }
        for cell in notebook_cells(notebook):
            exec(compile(cell, notebook, "exec"), namespace)  # noqa: S102 - running our own notebooks

    return run
