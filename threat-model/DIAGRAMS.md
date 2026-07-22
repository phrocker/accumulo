<!--
  GENERATED FILE — do not edit by hand.
  Regenerate with: python3 threat-model/gen-diagram.py
-->
# Threat Model Diagrams

Data-flow diagrams generated from the OTM models. Node shapes: stadium = external entity, rounded = process, cylinder = datastore, hexagon = web application. Subgraphs are trust zones.

## Apache Accumulo

Source: [`accumulo.otm.yaml`](accumulo.otm.yaml)

```mermaid
flowchart LR
  subgraph tz_public["Public / Client Network (trust 10)"]
    client_app(["Client Application / Shell"])
  end
  subgraph tz_operator["Operator / Admin Network (trust 40)"]
    monitor_user(["Operator (Browser / Admin CLI)"])
  end
  subgraph tz_cluster["Accumulo Server Cluster (trust 80)"]
    manager("Manager")
    tserver("Tablet Server")
    scan_server("Scan Server (sserver)")
    compactor("External Compactor")
    gc("Garbage Collector")
    monitor{{"Monitor (Web UI)"}}
  end
  subgraph tz_zookeeper["ZooKeeper Ensemble (trust 70)"]
    zookeeper[("ZooKeeper Ensemble")]
  end
  subgraph tz_hdfs["HDFS Storage (trust 70)"]
    hdfs[("HDFS (NameNode + DataNodes)")]
  end

  client_app -->|"Client RPC (scan / write / admin)"| tserver
  client_app -->|"Client Eventual-Consistency Scan"| scan_server
  client_app -->|"Client Admin RPC (create/drop, permissions)"| manager
  manager -->|"Manager ⇄ TabletServer coordination"| tserver
  manager -->|"Compaction dispatch (Coordinator ⇄ Compactor)"| compactor
  tserver -->|"Server ⇄ ZooKeeper (locks, config, users)"| zookeeper
  tserver -->|"Server ⇄ HDFS (RFile / WAL I/O)"| hdfs
  compactor -->|"Compactor ⇄ HDFS (read/write RFiles)"| hdfs
  gc -->|"GC ⇄ HDFS (delete unreferenced files)"| hdfs
  monitor_user -->|"Operator → Monitor UI"| monitor
```

## Apache Accumulo — Column Visibility Subsystem

Source: [`column-visibility.otm.yaml`](column-visibility.otm.yaml)

```mermaid
flowchart LR
  subgraph tz_public["Public / Client Network (trust 10)"]
    client_scan(["Scanning Client"])
    client_writer(["Writing Client"])
  end
  subgraph tz_cluster["Accumulo Server Cluster (trust 80)"]
    security_operation("SecurityOperation (auth gate)")
    visibility_filter("System VisibilityFilter (per-cell evaluation)")
    visibility_constraint("VisibilityConstraint (write gate)")
    bulk_import("Bulk Import (externally-generated RFiles)")
  end
  subgraph tz_zookeeper["ZooKeeper Ensemble (trust 70)"]
    zk_auth_store[("ZK Security Store (users / auths / perms)")]
  end
  subgraph tz_hdfs["HDFS Storage (trust 70)"]
    rfile_store[("RFiles (cells + visibility labels)")]
  end

  client_scan -->|"Scan request with presented Authorizations"| security_operation
  security_operation -->|"Look up user's granted authorizations / permissions"| zk_auth_store
  security_operation -->|"Validated auths handed to per-cell filter"| visibility_filter
  visibility_filter -->|"Read cells + ColumnVisibility labels"| rfile_store
  client_writer -->|"Write mutation carrying a ColumnVisibility"| visibility_constraint
  bulk_import -->|"Import externally-generated RFiles"| rfile_store
```

