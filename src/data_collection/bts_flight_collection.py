"""BTS collection entry point; implementation lives in flightdelay.collection."""

import sys
from collections.abc import Sequence

from flightdelay.cli import main as run_pipeline


def main(argv: Sequence[str] | None = None) -> int:
    arguments = sys.argv[1:] if argv is None else argv
    return run_pipeline(["collect-flights", *arguments])


if __name__ == "__main__":
    raise SystemExit(main())
