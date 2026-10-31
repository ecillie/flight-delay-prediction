from pathlib import Path


class SourceChangedError(RuntimeError):
    def __init__(self, *, path: Path) -> None:
        self.path = path
        super().__init__(f"Legacy source changed during migration: {path}")


class MigrationConflictError(RuntimeError):
    def __init__(self, *, key: str) -> None:
        self.key = key
        super().__init__(f"Partition belongs to a different source snapshot: {key}")


class MigrationParityError(AssertionError):
    def __init__(self, *, key: str, reason: str) -> None:
        self.key = key
        self.reason = reason
        super().__init__(f"Migration parity failed for {key}: {reason}")
