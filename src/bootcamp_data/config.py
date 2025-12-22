
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Paths:
    root: Path
    raw: Path
    cache: Path 
    Processed: Path
    external: Path
    reports: Path



def make_paths(root: Path) -> Paths:
    data = root/ "data"
    return Paths(
        root=root,
        raw=data/"raw",
        cache=data/"cache",
        Processed=data/"Processed",
        external=data/"external",
        reports=data/"reports"
    )




