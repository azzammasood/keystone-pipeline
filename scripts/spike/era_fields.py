"""Phase 1 spike: field-presence matrix per event type across sample hours.

Reads data/spike/hours/*.json and writes data/spike/era_fields.md: for each event type, the
paths (depth <= 3) whose presence differs across sample hours, with the share present.
"""

import json

from keystone.paths import DATA_DIR


def main() -> None:
    files = sorted((DATA_DIR / "spike" / "hours").glob("*.json"))
    hours = [f.stem for f in files]
    data = {f.stem: json.loads(f.read_text(encoding="utf-8")) for f in files}
    types = sorted({t for d in data.values() for t in d["types"]})
    lines = ["# Field presence by event type and sample hour (share of events)", ""]
    for t in types:
        present = {h: data[h]["fields"].get(t, {}) for h in hours}
        seen = [h for h in hours if t in data[h]["types"]]
        paths = sorted({p for h in seen for p in present[h]})
        varying = [p for p in paths if len({present[h].get(p, 0) >= 0.5 for h in seen}) > 1]
        lines += [
            f"## {t}",
            "",
            "events: " + ", ".join(f"{h}={data[h]['types'].get(t, 0)}" for h in hours),
            "",
        ]
        if not varying:
            lines += ["(no presence differences)", ""]
            continue
        lines.append("| path | " + " | ".join(h[:10] for h in seen) + " |")
        lines.append("|---" * (len(seen) + 1) + "|")
        for p in varying:
            lines.append(
                f"| {p} | "
                + " | ".join(f"{present[h].get(p, 0):.2f}".rstrip("0").rstrip(".") for h in seen)
                + " |"
            )
        lines.append("")
    out = DATA_DIR / "spike" / "era_fields.md"
    out.write_text("\n".join(lines), encoding="utf-8")
    print(f"wrote {out} ({len(lines)} lines, {len(types)} types)")


if __name__ == "__main__":
    main()
