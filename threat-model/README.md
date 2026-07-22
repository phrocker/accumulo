<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements. See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
-->
# Apache Accumulo — Machine-Readable Threat Model

A version-controlled, machine-readable threat model for Accumulo (4.x / `main`).
It lives next to the code so it can be reviewed in pull requests, diffed over
time, and validated in CI — instead of drifting in a wiki or diagram tool.

## Files

| File | Purpose |
| --- | --- |
| `accumulo.otm.yaml` | The threat model itself, in [Open Threat Model (OTM) 0.2.0](https://github.com/iriusrisk/OpenThreatModel) format. |
| `accumulo.otm.schema.json` | JSON Schema (Draft 2020-12) for the OTM subset used here. |
| `validate.py` | Validates structure **and** referential integrity. |

## Why OTM

OTM is a tool-agnostic, declarative spec (plain YAML/JSON). It describes trust
zones, components, dataflows, threats, and mitigations as data — so the model is
readable by humans and by tooling (e.g. IriusRisk import, or custom scripts that
generate diagrams or reports). No proprietary format, no binary diagrams.

## Model structure

- **trustZones** — trust boundaries with a `trustRating` (1 untrusted … 100 trusted):
  public/client, operator, server cluster, ZooKeeper, HDFS.
- **components** — processes and datastores: Manager, TabletServer, ScanServer,
  Compactor, GC, Monitor, ZooKeeper, HDFS, plus external client/operator entities.
  Each attaches the threats it is exposed to, with the mitigations addressing them.
- **dataflows** — client RPC, inter-server coordination, ZooKeeper and HDFS I/O,
  Monitor UI — tagged with protocol/auth (SASL, delegation tokens, TLS).
- **threats** — STRIDE-categorized, with CWE references and a likelihood/impact
  risk score.
- **mitigations** — Accumulo's actual controls (column-visibility enforcement,
  SASL/Kerberos, ZK ACLs, on-disk encryption, …), cross-referenced from threats.

Threat/mitigation `state` fields (`exposed`, `partially-implemented`,
`implemented`) make gaps explicit and reviewable.

## Validate

```bash
# PyYAML required; jsonschema optional (adds structural validation)
python3 -m pip install pyyaml jsonschema
python3 threat-model/validate.py
```

`validate.py` checks the JSON Schema **and** referential integrity that a schema
cannot express: every `component.parent`, dataflow `source`/`destination`, and
threat/mitigation reference must resolve to a defined `id`. Orphan threats (not
attached to any component) are reported as warnings.

## Scope & status

This is a **whole-system skeleton**: the major boundaries and a representative
set of threats/mitigations are in place. It is intended to be extended
per-subsystem over time (deeper RPC/auth, delegation tokens, column visibility,
crypto, FATE, etc.). It is **not** a security audit or a completeness guarantee.

To report a security vulnerability, follow the ASF process at
<https://accumulo.apache.org/contact-us/> — do **not** use public issues.

## Extending the model

1. Edit `accumulo.otm.yaml` (add components/threats/mitigations, deepen a subsystem).
2. Run `python3 threat-model/validate.py` until it passes.
3. Open a PR — the model change is reviewed alongside the code change.
