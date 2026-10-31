# Python rules for FlightDelayModel

## Scope and precedence

Read `~/.codex/python-guidelines.md` before creating, editing, reviewing,
debugging, or executing Python code, including notebook cells.

This repository intentionally narrows the application of those guidelines:
the software engineering process applies to engineering work only. For data
exploration and modeling, the research workflow below overrides conflicting
global Python design, layout, documentation, and verification requirements.
This is the repository owner's explicit exception to the global all-Python
scope; continue to follow other applicable instructions.

Classify work by its purpose, not its file extension or directory. A notebook
can contain engineering work, and a Python module can be a research experiment.
For mixed tasks, apply each workflow to the relevant code.

## Software engineering work

Follow `~/.codex/python-guidelines.md` for building, maintaining, reviewing,
debugging, or running software infrastructure, including:

- Data collection, ingestion, ETL, scheduled jobs, and reusable data pipelines.
- Backend services, APIs, database integration, and operational tooling.
- Shared application libraries and production training or inference services.
- Tests and supporting code for these systems.

Use the prescribed design standards and engineering verification process,
including Ruff linting, Ruff formatting, ty type checking, and pytest. Report
checks that could not be completed. Preserve compatible existing conventions;
do not restructure unrelated code merely to enforce a preferred layout.

## Data exploration and modeling

Do not impose the global software engineering process on:

- Exploratory data analysis, ad hoc queries, visualizations, and statistics.
- Research data preparation and experimental feature engineering.
- Building, training, tuning, running, comparing, and evaluating models.
- Research notebooks and scripts supporting those activities.

For this work:

- Favor clear, direct scientific code and fast iteration. Classes, dependency
  injection, package restructuring, exhaustive typing, and other engineering
  patterns are not required.
- Allow ordinary notebook and scientific Python idioms, including top-level
  cells, mutable intermediate data, inline calculations, comments, and
  explanatory docstrings.
- Do not require Ruff, ty, pytest, or the global diff checklist solely for
  exploration or modeling changes. Use focused checks appropriate to the task,
  such as running affected cells or a small model experiment.
- Validate relevant data assumptions and model results. Avoid train/test
  leakage and record seeds and experiment settings when reproducibility matters.
- Preserve existing notebook structure, metadata, and outputs unless a change
  is necessary for the task. Do not run expensive training or full notebook
  execution merely to satisfy an engineering checklist.
- Use the project's existing environment and dependency workflow. Report what
  was actually run and any validation that remains incomplete.

Research code enters the engineering workflow when the task explicitly turns
it into maintained infrastructure, a reusable pipeline, or a deployed service.
Training or running a model alone does not trigger that transition.
