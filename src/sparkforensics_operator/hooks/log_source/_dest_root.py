from __future__ import annotations

import shutil
import tempfile
from pathlib import Path


def dest_for(dest_root: Path, source_path: str | Path) -> Path:
    """The dest_root/<basename> convention shared by every LogSourceHook that
    stages a single file: FilesystemLogSourceHook (local copy) and
    SFTPLogSourceHook (remote-to-local fetch)."""
    return dest_root / Path(source_path).name


def make_dest_root(dest_dir: str | None) -> tuple[Path, Path | None]:
    """Resolves the directory a hook stages files into: dest_dir if the
    caller gave one (a shared, caller-managed directory — the second tuple
    element is None so cleanup() never touches it), else a fresh private
    temp dir this hook owns (returned as both elements, so the caller can
    rmtree it later, on error or in cleanup())."""
    if dest_dir is not None:
        root = Path(dest_dir)
        return root, None
    owned = Path(tempfile.mkdtemp(prefix="sparkforensics-"))
    return owned, owned


def remove_if_owned(owned_temp_root: Path | None) -> None:
    if owned_temp_root is not None:
        shutil.rmtree(owned_temp_root, ignore_errors=True)
