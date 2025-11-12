"""Run compute pipeline (features + scores)."""

from __future__ import annotations

from core.db import init_db
from compute.aggregate import run_compute


def main():
    init_db()
    return run_compute()


if __name__ == "__main__":
    print(main())
