"""Run the ODCS data contracts in `contracts/` with datacontract-cli (DuckDB on local Parquet).

datacontract-cli does not check column types on Parquet yet (it reports a warning per field);
`tests/test_contracts.py` checks the contract types against the Arrow schemas instead, so those
warnings are expected. Any other warning, a failure or an error fails the run.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from keystone.paths import CONTRACTS_DIR

PARQUET_TYPE_WARNING = "Checking types in parquet files is not supported yet."


@dataclass(frozen=True)
class ContractCheck:
    result: str  # passed | warning | failed | error
    field: str | None
    name: str
    reason: str


@dataclass(frozen=True)
class ContractResult:
    contract: str
    rows_path: str
    checks: list[ContractCheck]

    @property
    def problems(self) -> list[ContractCheck]:
        return [
            c
            for c in self.checks
            if c.result in ("failed", "error")
            or (c.result == "warning" and c.reason != PARQUET_TYPE_WARNING)
        ]

    @property
    def ok(self) -> bool:
        return not self.problems


def contract_files(directory: Path = CONTRACTS_DIR) -> list[Path]:
    return sorted(directory.glob("*.odcs.yaml"))


def load_contract(path: Path) -> dict[str, Any]:
    data: dict[str, Any] = yaml.safe_load(path.read_text(encoding="utf-8"))
    return data


def run_contract(
    path: Path, rows_glob: str | None = None, database: str | None = None
) -> ContractResult:
    """Test one contract against its `local` server: Parquet files (`rows_glob` overrides the
    path) or a DuckDB database file (`database` overrides it)."""
    from datacontract.data_contract import DataContract  # heavy; optional `quality` group

    spec = load_contract(path)
    server = spec["servers"][0]
    if rows_glob is not None:
        server["path"] = rows_glob
    if database is not None:
        server["database"] = database
    run = DataContract(data_contract_str=yaml.safe_dump(spec), server=server["server"]).test()
    checks = [
        ContractCheck(
            result=str(getattr(c.result, "value", c.result)),
            field=c.field,
            name=c.name or "",
            reason=str(c.reason or ""),
        )
        for c in run.checks or []
    ]
    return ContractResult(spec["id"], str(server.get("path") or server["database"]), checks)
