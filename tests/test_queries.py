import importlib
import json
import pytest

def load_parse_loki_results():
    """Load and return :func:`parse_loki_results` from ``queries.py``."""

    from pathlib import Path

    spec = importlib.util.spec_from_file_location(
        "queries",
        Path(__file__).resolve().parents[1] / "scripts" / "queries.py",
    )
    queries = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(queries)
    return queries.parse_loki_results


def test_parse_empty():
    parse = load_parse_loki_results()
    df = parse("")
    assert df.to_dict() == []
    assert df._columns == ["group", "detector", "exposure"]


def test_parse_sample():
    parse = load_parse_loki_results()
    outer1 = {"line": json.dumps({"group": "g1", "detector": 1, "exposures": [101, 102]})}
    outer2 = {"line": json.dumps({"group": "g2", "detector": 2, "exposures": [201]})}
    results = "\n".join([json.dumps(outer1), json.dumps(outer2)])
    df = parse(results)
    assert df.to_dict() == [
        {"group": "g1", "detector": 1, "exposure": 101},
        {"group": "g2", "detector": 2, "exposure": 201},
    ]

