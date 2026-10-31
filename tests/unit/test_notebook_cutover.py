import json
from pathlib import Path

import pytest

from flightdelay.migration.notebooks import NotebookEdit, prepare_edit


class TestNotebookCutover:
    @pytest.fixture
    def original(self) -> dict[str, object]:
        return {
            "metadata": {"language_info": {"version": "3.11.5"}},
            "nbformat": 4,
            "nbformat_minor": 2,
            "cells": [
                {
                    "cell_type": "code",
                    "execution_count": 7,
                    "metadata": {"tag": "keep"},
                    "outputs": [{"output_type": "stream", "text": ["saved output\n"]}],
                    "source": [
                        "import sqlite3\n",
                        "from pathlib import Path\n",
                        'DB_PATH = Path("../data/database/flights.sqlite")\n',
                        "conn = sqlite3.connect(DB_PATH)\n",
                    ],
                },
                {
                    "cell_type": "code",
                    "execution_count": 8,
                    "metadata": {},
                    "outputs": [],
                    "source": [
                        'cancelled_query = """\n',
                        "SELECT * FROM raw_ontime_flights\n",
                        "WHERE Cancelled IS NOT NULL\n",
                        "AND rowid % 10 = 0;\n",
                        '"""\n',
                        "cancelled_df = pd.read_sql_query(cancelled_query, conn)\n",
                    ],
                },
            ],
        }

    @pytest.fixture
    def edit(self, tmp_path: Path, original: dict[str, object]) -> NotebookEdit:
        path = tmp_path.joinpath("notebook.ipynb")
        path.write_text(json.dumps(original, indent=1) + "\n")
        return prepare_edit(path)

    def test_keeps_saved_outputs_and_metadata(
        self, edit: NotebookEdit, original: dict[str, object]
    ) -> None:
        changed = json.loads(edit.after)
        assert changed["metadata"] == original["metadata"]
        assert changed["cells"][0]["execution_count"] == 7
        assert changed["cells"][0]["outputs"] == [
            {"output_type": "stream", "text": ["saved output\n"]}
        ]
        assert "open_research_connection" in "".join(changed["cells"][0]["source"])
        assert "ORDER BY rowid" in "".join(changed["cells"][1]["source"])

    def test_preparation_does_not_write_original(self, edit: NotebookEdit) -> None:
        assert edit.path.read_text() == edit.before
