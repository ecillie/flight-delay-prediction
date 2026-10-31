from pathlib import Path


class PartitionError(ValueError):
    def __init__(self, *, partition: str, reason: str) -> None:
        self.partition = partition
        self.reason = reason
        super().__init__(f"Invalid partition {partition}: {reason}")


class DataValidationError(ValueError):
    def __init__(self, *, dataset: str, reason: str) -> None:
        self.dataset = dataset
        self.reason = reason
        super().__init__(f"Invalid {dataset} data: {reason}")


class StorageSpaceError(RuntimeError):
    def __init__(self, *, available: int, required: int) -> None:
        self.available = available
        self.required = required
        super().__init__(f"Storage allowance exhausted: {available}; need {required}")


class WriterBusyError(RuntimeError):
    def __init__(self, *, path: Path) -> None:
        self.path = path
        super().__init__(f"Another publisher holds {path}")


class IntegrityError(RuntimeError):
    def __init__(self, *, path: Path, reason: str) -> None:
        self.path = path
        self.reason = reason
        super().__init__(f"Partition integrity failure at {path}: {reason}")


class MissingDatasetError(RuntimeError):
    def __init__(self, *, dataset: str) -> None:
        self.dataset = dataset
        super().__init__(
            f"No committed {dataset} partitions; collect or migrate them first"
        )
