#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
"""Generate Mermaid data-flow diagrams from the OTM threat models.

Renders each *.otm.yaml as a Mermaid flowchart: trust zones become subgraphs,
components become shaped nodes (external entity / process / datastore / web app),
and dataflows become labelled edges. GitHub renders Mermaid in Markdown, so the
output drops straight into DIAGRAMS.md.

Usage:
    python3 threat-model/gen-diagram.py            # regenerate DIAGRAMS.md
    python3 threat-model/gen-diagram.py --check     # fail if DIAGRAMS.md is stale

Requires PyYAML.
"""
from __future__ import annotations

import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
OUT = HERE / "DIAGRAMS.md"

# Node shape per OTM component type: (open, close) Mermaid delimiters.
SHAPES = {
    "external-entity": ("([", "])"),   # stadium
    "process": ("(", ")"),             # rounded
    "datastore": ("[(", ")]"),         # cylinder
    "web-application": ("{{", "}}"),   # hexagon
}
DEFAULT_SHAPE = ("[", "]")


def load_yaml(path: Path):
    try:
        import yaml  # type: ignore
    except ImportError:
        sys.exit("ERROR: PyYAML is required (pip install pyyaml)")
    with path.open() as fh:
        return yaml.safe_load(fh)


def nid(raw: str) -> str:
    """Mermaid-safe node id."""
    return raw.replace("-", "_")


def esc(text: str) -> str:
    """Escape a label for a Mermaid node/edge."""
    return text.replace('"', "&quot;").replace("\n", " ").strip()


def render(model: dict) -> str:
    zones = {z["id"]: z for z in model.get("trustZones", [])}
    comps = model.get("components", [])
    by_zone: dict[str, list[dict]] = {zid: [] for zid in zones}
    orphans: list[dict] = []
    for c in comps:
        zid = c.get("parent", {}).get("trustZone")
        (by_zone.get(zid, orphans) if zid in zones else orphans).append(c)

    lines = ["flowchart LR"]

    for zid, zone in zones.items():
        rating = zone.get("risk", {}).get("trustRating", "?")
        title = esc(f'{zone["name"]} (trust {rating})')
        lines.append(f'  subgraph {nid(zid)}["{title}"]')
        for c in by_zone[zid]:
            o, cl = SHAPES.get(c.get("type", ""), DEFAULT_SHAPE)
            lines.append(f'    {nid(c["id"])}{o}"{esc(c["name"])}"{cl}')
        lines.append("  end")

    for c in orphans:
        o, cl = SHAPES.get(c.get("type", ""), DEFAULT_SHAPE)
        lines.append(f'  {nid(c["id"])}{o}"{esc(c["name"])}"{cl}')

    lines.append("")
    for d in model.get("dataflows", []):
        label = esc(d.get("name", ""))
        lines.append(f'  {nid(d["source"])} -->|"{label}"| {nid(d["destination"])}')

    return "\n".join(lines)


def build_doc() -> str:
    models = sorted(HERE.glob("*.otm.yaml"))
    parts = [
        "<!--",
        "  GENERATED FILE — do not edit by hand.",
        "  Regenerate with: python3 threat-model/gen-diagram.py",
        "-->",
        "# Threat Model Diagrams",
        "",
        "Data-flow diagrams generated from the OTM models. Node shapes: "
        "stadium = external entity, rounded = process, cylinder = datastore, "
        "hexagon = web application. Subgraphs are trust zones.",
        "",
    ]
    for path in models:
        model = load_yaml(path)
        parts += [
            f"## {model['project']['name']}",
            "",
            f"Source: [`{path.name}`]({path.name})",
            "",
            "```mermaid",
            render(model),
            "```",
            "",
        ]
    return "\n".join(parts) + "\n"


def main() -> int:
    doc = build_doc()
    if "--check" in sys.argv[1:]:
        current = OUT.read_text() if OUT.exists() else ""
        if current != doc:
            print("STALE: DIAGRAMS.md is out of date — run gen-diagram.py")
            return 1
        print("OK — DIAGRAMS.md is up to date.")
        return 0
    OUT.write_text(doc)
    print(f"Wrote {OUT.name} ({len(doc.splitlines())} lines).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
