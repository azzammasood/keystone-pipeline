"""Compare `pipeline_metrics.json` with what Grafana shows (gate: equal within rounding).

The check reads the provisioned dashboard from Grafana's API, runs each panel's query through
Grafana's own data-source endpoint (`/api/ds/query`, so the path is Grafana -> Prometheus ->
OpenTelemetry collector), and compares every value with the JSON path named in the panel's
description (`pipeline_metrics: <path>`). Stage panels are keyed by stage name and compared
with the last record of that stage in `last_run`. The stage gauges are recorded by each stage
as it ends, independently of the JSON, which is built later from the run log.
"""

import re
from dataclasses import dataclass
from typing import Any

import httpx

DASHBOARD_UID = "keystone-pipeline"
_PATH = re.compile(r"pipeline_metrics:\s*([\w.\[\]]+)")
# The JSON rounds durations and rates to 3 decimals (at most 0.0005 off); counts are exact.
TOLERANCE = 0.001


@dataclass(frozen=True)
class Comparison:
    panel: str
    key: str
    json_value: float | None
    grafana_value: float | None

    @property
    def ok(self) -> bool:
        if self.json_value is None or self.grafana_value is None:
            return self.json_value is None and self.grafana_value is None
        return abs(self.json_value - self.grafana_value) <= TOLERANCE


def _get(doc: dict[str, Any], path: str) -> Any:
    cur: Any = doc
    for part in path.split("."):
        cur = cur[part]
    return cur


def _query(client: httpx.Client, expr: str) -> dict[str, float]:
    body = {
        "queries": [
            {
                "refId": "A",
                "datasource": {"type": "prometheus", "uid": "prometheus"},
                "expr": expr,
                "instant": True,
                "range": False,
            }
        ],
        "from": "now-30d",
        "to": "now",
    }
    r = client.post("/api/ds/query", json=body)
    r.raise_for_status()
    out: dict[str, float] = {}
    for frame in r.json()["results"]["A"].get("frames", []):
        fields = frame["schema"]["fields"]
        values = frame["data"]["values"]
        value_field = next(i for i, f in enumerate(fields) if f["type"] == "number")
        labels = fields[value_field].get("labels") or {}
        if values[value_field]:
            out[labels.get("stage", "")] = float(values[value_field][-1])
    return out


def compare(doc: dict[str, Any], url: str = "http://127.0.0.1:3000") -> list[Comparison]:
    with httpx.Client(base_url=url, timeout=30) as client:
        dash = client.get(f"/api/dashboards/uid/{DASHBOARD_UID}")
        dash.raise_for_status()
        panels = dash.json()["dashboard"]["panels"]
        out = []
        last_stage = {s["name"]: s for s in doc["last_run"]["stages"]}
        for panel in panels:
            m = _PATH.search(panel.get("description", ""))
            if not m:
                continue
            path = m.group(1)
            got = _query(client, panel["targets"][0]["expr"])
            if path.startswith("last_run.stages[]."):
                field = path.removeprefix("last_run.stages[].")
                for name, stage in last_stage.items():
                    value = stage[field]
                    out.append(
                        Comparison(
                            panel["title"],
                            name,
                            None if value is None else float(value),
                            got.get(name),
                        )
                    )
            else:
                value = _get(doc, path)
                out.append(
                    Comparison(
                        panel["title"], path, None if value is None else float(value), got.get("")
                    )
                )
        return out


def versions(
    grafana: str = "http://127.0.0.1:3000", prometheus: str = "http://127.0.0.1:9090"
) -> dict[str, str]:
    """Versions reported by the running services (engines[] evidence)."""
    out = {}
    try:
        out["grafana"] = str(httpx.get(f"{grafana}/api/health", timeout=5).json()["version"])
        info = httpx.get(f"{prometheus}/api/v1/status/buildinfo", timeout=5).json()
        out["prometheus"] = str(info["data"]["version"])
        q = httpx.get(
            f"{prometheus}/api/v1/query",
            params={"query": 'target_info{job="otel-collector"}'},
            timeout=5,
        ).json()
        for res in q["data"]["result"]:  # the collector's own resource, scraped from :8888
            ver = res["metric"].get("service.version")
            if ver:
                out["otel-collector"] = ver
    except (httpx.HTTPError, KeyError, ValueError):
        pass
    return out
