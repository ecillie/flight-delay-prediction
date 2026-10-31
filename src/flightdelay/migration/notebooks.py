import argparse
import hashlib
import json
import subprocess
import sys
from collections.abc import Sequence
from contextlib import redirect_stdout
from dataclasses import dataclass
from enum import IntEnum
from io import StringIO
from pathlib import Path

import duckdb
import pandas as pd

from flightdelay.migration.domain import MigrationOptions
from flightdelay.migration.errors import MigrationConflictError
from flightdelay.migration.services import save_evidence
from flightdelay.storage.domain import StorageOptions
from flightdelay.storage.store import create_store


class ExitCode(IntEnum):
    SUCCESS = 0


@dataclass(slots=True, kw_only=True, frozen=True)
class NotebookOptions:
    repository: Path = Path(".")
    apply: bool = False
    verify: bool = False
    verify_cell: int | None = None


@dataclass(slots=True, kw_only=True, frozen=True)
class NotebookEdit:
    path: Path
    before: str
    after: str
    changed_cells: tuple[int, ...]


def prepare_edit(path: Path) -> NotebookEdit:
    before = path.read_text()
    original = json.loads(before)
    updated = json.loads(before)
    changed = []
    for index, cell in enumerate(updated["cells"]):
        text = "".join(cell.get("source", []))
        replacement = text.replace("import sqlite3\n", "")
        if "sqlite3.connect(DB_PATH)" in replacement:
            replacement = replacement.replace(
                "from pathlib import Path\n",
                "from pathlib import Path\n\n"
                "from flightdelay.storage.analytics import open_research_connection\n",
            )
            replacement = replacement.replace(
                'DB_PATH = Path("../data/database/flights.sqlite")',
                'DATA_ROOT = Path("../data")',
            )
            replacement = replacement.replace(
                "sqlite3.connect(DB_PATH)", "open_research_connection(DATA_ROOT)"
            )
        for name in ("cancelled", "delayed", "diverted"):
            replacement = replacement.replace(
                f"pd.read_sql_query({name}_query, conn)",
                f"conn.execute({name}_query).fetchdf()",
            )
        replacement = replacement.replace(
            "AND rowid % 10 = 0;", "AND rowid % 10 = 0\nORDER BY rowid;"
        )
        if replacement != text:
            cell["source"] = replacement.splitlines(keepends=True)
            changed.append(index)
    if updated["metadata"] != original["metadata"] or len(updated["cells"]) != len(
        original["cells"]
    ):
        raise MigrationConflictError(key=f"notebook structure: {path}")
    for before_cell, after_cell in zip(
        original["cells"], updated["cells"], strict=True
    ):
        original_fields = {
            key: value for key, value in before_cell.items() if key != "source"
        }
        updated_fields = {
            key: value for key, value in after_cell.items() if key != "source"
        }
        if original_fields != updated_fields:
            raise MigrationConflictError(key=f"notebook outputs/metadata: {path}")
    return NotebookEdit(
        path=path,
        before=before,
        after=json.dumps(updated, indent=1) + "\n",
        changed_cells=tuple(changed),
    )


def apply_notebooks(options: NotebookOptions) -> list[dict[str, object]]:
    evidence = options.repository.joinpath("docs/data-validation")
    if options.apply:
        validation = json.loads(
            evidence.joinpath("phase-4-validation.json").read_text()
        )
        inventory = json.loads(evidence.joinpath("phase-4-inventory.json").read_text())
        store = create_store(StorageOptions(root=options.repository.joinpath("data")))
        generations = {
            f"{record.dataset}:{record.partition_key}": record.generation
            for record in store.manifest.records()
        }
        if (
            not validation.get("complete")
            or validation.get("source_identity") != inventory.get("source_identity")
            or validation.get("generations") != generations
        ):
            raise MigrationConflictError(
                key="notebook cutover requires current full parity validation"
            )
    edits = [
        prepare_edit(options.repository.joinpath("notebooks", name))
        for name in ("01_explore_database.ipynb", "02_explore_models.ipynb")
    ]
    results: list[dict[str, object]] = []
    for edit in edits:
        if options.apply and len(edit.changed_cells) > 0:
            if edit.path.read_text() != edit.before:
                raise MigrationConflictError(
                    key=f"notebook changed during preparation: {edit.path}"
                )
            edit.path.write_text(edit.after)
        results.append(
            {
                "path": str(edit.path),
                "changed_cells": edit.changed_cells,
                "before_sha256": hashlib.sha256(edit.before.encode()).hexdigest(),
                "after_sha256": hashlib.sha256(edit.after.encode()).hexdigest(),
                "metadata_outputs_execution_counts_preserved": True,
                "applied": options.apply,
            }
        )
    if options.apply:
        save_evidence(MigrationOptions(evidence=evidence), "notebooks", results)
    return results


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Prepare or apply parity-gated notebook connection changes"
    )
    parser.add_argument("--repository", type=Path, default=NotebookOptions().repository)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--verify", action="store_true")
    parser.add_argument("--verify-cell", type=int, choices=range(4))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    options = NotebookOptions(**vars(build_parser().parse_args(argv)))
    if options.verify_cell is not None:
        print(json.dumps(execute_loading_cell(options), indent=2))
        return ExitCode.SUCCESS
    if options.verify:
        print(json.dumps(verify_notebooks(options), indent=2))
        return ExitCode.SUCCESS
    print(json.dumps(apply_notebooks(options), indent=2))
    return ExitCode.SUCCESS


def execute_loading_cell(options: NotebookOptions) -> dict[str, object]:
    index = options.verify_cell
    assert index is not None
    name = "01_explore_database.ipynb" if index == 0 else "02_explore_models.ipynb"
    path = options.repository.joinpath("notebooks", name)
    notebook = json.loads(path.read_text())
    namespace: dict[str, object] = {"__name__": "__phase4_notebook__"}
    with redirect_stdout(StringIO()):
        exec(
            compile("".join(notebook["cells"][0]["source"]), str(path), "exec"),
            namespace,
        )
        connection = namespace["conn"]
        assert isinstance(connection, duckdb.DuckDBPyConnection)
        try:
            if index == 0:
                return {
                    "notebook": name,
                    "cell": index,
                    "views": {
                        table: connection.execute(
                            f"SELECT COUNT(*) FROM {table}"
                        ).fetchall()[0][0]
                        for table in (
                            "raw_ontime_flights",
                            "weather_hourly",
                            "airports",
                        )
                    },
                }
            exec(
                compile("".join(notebook["cells"][index]["source"]), str(path), "exec"),
                namespace,
            )
            target = ("cancelled", "delayed", "diverted")[index - 1]
            frame = namespace[f"{target}_df"]
            assert isinstance(frame, pd.DataFrame)
            return {
                "notebook": name,
                "cell": index,
                "sample": target,
                "rows": len(frame),
                "columns": len(frame.columns),
                "target_counts": {
                    str(key): int(value)
                    for key, value in frame[f"target_{target}"].value_counts().items()
                },
                "year_counts": {
                    str(key): int(value)
                    for key, value in frame["Year"].value_counts().items()
                },
            }
        finally:
            connection.close()


def verify_notebooks(options: NotebookOptions) -> list[dict[str, object]]:
    root = options.repository.resolve()
    results = []
    for index in range(4):
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "flightdelay.migration.notebooks",
                "--repository",
                str(root),
                "--verify-cell",
                str(index),
            ],
            cwd=root.joinpath("notebooks"),
            text=True,
            capture_output=True,
            timeout=300,
            check=True,
        )
        results.append(json.loads(result.stdout))
    evidence = root.joinpath("docs/data-validation")
    validation = json.loads(evidence.joinpath("phase-4-validation.json").read_text())
    expected = {item["sample"]: item for item in validation["samples"]}
    for result in results[1:]:
        reference = expected[result["sample"]]
        if (
            result["rows"] != reference["rows"]
            or result["target_counts"] != reference["target_counts"]
            or result["year_counts"] != reference["year_counts"]
        ):
            raise MigrationConflictError(
                key=f"notebook sample differs: {result['sample']}"
            )
    save_evidence(
        MigrationOptions(evidence=evidence),
        "notebook-execution",
        {"cells": results, "complete": True, "models_trained": False},
    )
    return results


if __name__ == "__main__":
    raise SystemExit(main())
