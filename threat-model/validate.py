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
"""Validate the Accumulo OTM threat model.

Two layers:
  1. JSON Schema structural validation (if `jsonschema` is installed).
  2. Referential-integrity checks that a schema cannot express: every
     component.parent, dataflow endpoint, and threat/mitigation reference
     must resolve to a defined id.

Usage:
    python3 threat-model/validate.py            # validate every *.otm.yaml here
    python3 threat-model/validate.py path.yaml  # validate a specific file

Exit code 0 = valid, 1 = problems found. Requires PyYAML; jsonschema optional.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
SCHEMA = HERE / "accumulo.otm.schema.json"


def load_yaml(path: Path):
    try:
        import yaml  # type: ignore
    except ImportError:
        sys.exit("ERROR: PyYAML is required (pip install pyyaml)")
    with path.open() as fh:
        return yaml.safe_load(fh)


def schema_validate(model, errors: list[str]) -> None:
    try:
        import jsonschema  # type: ignore
    except ImportError:
        print("NOTE: jsonschema not installed — skipping structural validation.")
        return
    schema = json.loads(SCHEMA.read_text())
    validator = jsonschema.Draft202012Validator(schema)
    for err in sorted(validator.iter_errors(model), key=lambda e: e.path):
        loc = "/".join(str(p) for p in err.path) or "<root>"
        errors.append(f"[schema] {loc}: {err.message}")


def ids(items) -> set[str]:
    return {i["id"] for i in (items or []) if isinstance(i, dict) and "id" in i}


def ref_validate(model, errors: list[str]) -> None:
    zones = ids(model.get("trustZones"))
    comps = ids(model.get("components"))
    threats = ids(model.get("threats"))
    mitigations = ids(model.get("mitigations"))

    for c in model.get("components", []):
        parent = c.get("parent", {})
        tz, pc = parent.get("trustZone"), parent.get("component")
        if tz and tz not in zones:
            errors.append(f"[ref] component {c.get('id')}: unknown trustZone '{tz}'")
        if pc and pc not in comps:
            errors.append(f"[ref] component {c.get('id')}: unknown parent component '{pc}'")
        for t in c.get("threats", []):
            if t.get("threat") not in threats:
                errors.append(f"[ref] component {c.get('id')}: unknown threat '{t.get('threat')}'")
            for m in t.get("mitigations", []):
                if m.get("mitigation") not in mitigations:
                    errors.append(
                        f"[ref] component {c.get('id')}: unknown mitigation '{m.get('mitigation')}'"
                    )

    for d in model.get("dataflows", []):
        for endpoint in ("source", "destination"):
            if d.get(endpoint) not in comps:
                errors.append(
                    f"[ref] dataflow {d.get('id')}: unknown {endpoint} '{d.get(endpoint)}'"
                )

    # Coverage hints (warnings, not failures) — surfaced but don't fail the build.
    referenced_threats = {
        t["threat"]
        for c in model.get("components", [])
        for t in c.get("threats", [])
        if "threat" in t
    }
    for orphan in sorted(threats - referenced_threats):
        print(f"WARN: threat '{orphan}' is defined but not attached to any component")


def validate_one(path: Path) -> bool:
    if not path.exists():
        print(f"ERROR: model not found: {path}")
        return False
    model = load_yaml(path)

    errors: list[str] = []
    schema_validate(model, errors)
    ref_validate(model, errors)

    if errors:
        print(f"\nFAILED — {path.name}: {len(errors)} problem(s):")
        for e in errors:
            print(f"  - {e}")
        return False
    print(f"OK — {path.name} is valid "
          f"({len(model.get('components', []))} components, "
          f"{len(model.get('threats', []))} threats, "
          f"{len(model.get('mitigations', []))} mitigations).")
    return True


def main() -> int:
    if len(sys.argv) > 1:
        paths = [Path(a) for a in sys.argv[1:]]
    else:
        paths = sorted(HERE.glob("*.otm.yaml"))
    if not paths:
        sys.exit("ERROR: no *.otm.yaml models found")

    ok = all(validate_one(p) for p in paths)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
