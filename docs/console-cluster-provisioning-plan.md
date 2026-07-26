# Console cluster provisioning — Kubernetes (enterprise) plan

> **Status: future upgrade, not scheduled.** This is the design for letting the console *create and
> manage shardlite clusters* (not just connect to existing ones). It is intended as a **licensed
> enterprise capability**. See "Product tiering" below.

## Product tiering

| Tier | How a cluster comes to exist | Where it runs |
|---|---|---|
| **Individual / self-host** | `deploy/stack/shardlite-stack up` — one CLI brings up a 3-node cluster + console on Docker volumes. | One host, Docker. Already shipped. |
| **Enterprise (licensed)** | The console provisions clusters on demand: a **Clusters** UI creates a StatefulSet-backed shardlite cluster, manages its lifecycle, and auto-registers it as a connection. | Kubernetes. This document. |

The individual path stays the simple, unlicensed default. The Kubernetes provisioning below is the
paid upgrade — it is where the operational value (one-click clusters, lifecycle, scaling) and the
licensing gate live. The console already separates *connecting to* a cluster from *running* one, so
this adds a capability rather than reworking the core.

---

## Blockers / dependencies (know these first)

1. **shardlite has no StatefulSet story yet.** `serve` takes static `--node-id N` and
   `--peers 1=host:4600,…`. In k8s, stable identity comes from a *headless Service*
   (`shardlite-<id>-0.svc`, `-1`, …); node-id/peers must be **derived from the pod ordinal** by a small
   entrypoint shim in the image. That shim does not exist — it is slice 0.
2. **Scale-out / rebalance is gated on the shardlite dynamic-scaling core.** Bumping a StatefulSet's
   replicas adds pods, but the current deployment path has no safe dynamic join, replica bootstrap,
   or durable live-transfer lifecycle. So a new pod must hold **no primaries and take no writes**
   until the membership-through-rebalance slices in
   [dynamic-scaling-plan.md](dynamic-scaling-plan.md) are complete. Create/start/stop/destroy are
   deliverable now; **true scaling is not** — build the k8s mechanism, label it honestly, and wait on
   shardlite for data bootstrap and rebalance.
3. **The console must run in-cluster with RBAC.** Creating StatefulSets needs a ServiceAccount + a
   namespace-scoped Role. Safer than the Docker-socket alternative (namespaced, not root-on-host),
   but it means "deploy the console to k8s" becomes part of the plan.
4. **Stay sync — no `kube-rs`.** That crate is async/tokio and clashes with the console's sync stack
   (tiny_http/ureq). The console already links **rustls**, so talk to the k8s REST API directly with
   `ureq` + the in-cluster token/CA. No new runtime, consistent with the existing architecture.

---

## Architecture

```
Console (Deployment, in-cluster SA + namespace Role)
   │  k8s REST via ureq+rustls  (token @ /var/run/secrets/.../token, CA, KUBERNETES_SERVICE_HOST)
   ▼
per cluster:  Headless Service shardlite-<id>   (stable peer DNS)
              StatefulSet   shardlite-<id>  replicas=N, volumeClaimTemplates (PVC per pod)
                 entrypoint shim: node-id = ordinal+1, peers = other ordinals' DNS, --shards S
   │
   └─ console polls pods Ready + /v1/health (leader elected) ──► auto-registers a connection
                                                                 (seeds = in-cluster Service DNS)
```

New console pieces:
- **`k8s.rs`** — in-cluster client: apply/get/delete/scale for `StatefulSet`, `Service`,
  `PersistentVolumeClaim`, `Pod` (list/status). Just enough typed JSON over `ureq`.
- **Cluster registry** (`clusters.rs`) — persisted store of clusters the console created (id, name,
  namespace, desired nodes, routing mode, mutable split policy, phase). Current legacy clusters
  retain their immutable shard count; new linear-routing clusters begin small and grow internally.
  A created cluster auto-creates a **connection**, so it flows into the existing
  observe/query/assistant surface unchanged.
- **REST** — `POST/GET/DELETE /api/clusters`, `POST /api/clusters/<id>/{stop,start,scale}`.
  **Admin-only** (new `ManageClusters` permission, or reuse `ManageConnections`), audited, delete via
  **typed confirm** (matches the console's delete-always-confirms rule). License check gates create.
- **UI** — a **Clusters** section: a create wizard (name, node count, shard count — shard count
  locked with a "cannot change later" warning), lifecycle buttons, live status from k8s pod readiness
  + shardlite health.
- **Deploy artifacts** — SA + Role + RoleBinding + console Deployment/Service as a small **Helm
  chart**, parallel to `deploy/stack/` for Docker.

---

## Lifecycle → k8s mapping

| Console action | k8s | Notes |
|---|---|---|
| **Create** | apply Service + StatefulSet(replicas=N) → initialize/join → poll data ready + leader → register connection | New linear-routing clusters have no permanent shard-count choice |
| **Stop** | scale replicas → 0 | keeps PVCs (data safe) |
| **Start** | scale replicas → N | data intact from PVCs |
| **Destroy** | delete StatefulSet + Service + PVCs, drop connection | typed confirm |
| **Scale-out** | bump replicas → learner join → bootstrap → rebalance/split | complete only when capacity is active, not merely when the pod is Ready |
| **Scale-in** | cordon/drain highest ordinal → verify empty → remove member → lower replicas | never delete a pod that still owns a primary or required replica |

---

## Build order (each slice independently verifiable, on `kind`/minikube)

- **0 — StatefulSet-ready shardlite image.** Entrypoint derives node-id/peers from pod ordinal +
  headless DNS. *Done when:* a hand-written StatefulSet brings up a healthy N-node cluster,
  reachable in-cluster.
- **1 — `k8s.rs` client.** In-cluster auth + minimal REST. *Done when:* it can create/get/delete a
  throwaway object against a real kind cluster (a `--k8s-selftest` subcommand).
- **2 — Create + auto-register.** Registry + `POST /api/clusters` → apply → poll → connection appears
  healthy. *Done when:* one API call yields a browsable 3-node cluster.
- **3 — stop / start / destroy.** *Done when:* stop removes pods but keeps PVCs, start restores data,
  destroy removes everything + the connection.
- **4 — Clusters UI.** Wizard + lifecycle + status, end-to-end from the browser.
- **5 — Console-on-k8s.** RBAC + Deployment + Helm chart; console provisions from inside the cluster.
- **6 — Scale-out/in + rebalance *(blocked on the dynamic scaling plan)*.** Ship only when
  shardlite can dynamically join a node, bootstrap replicas, move/split shards through fenced
  cutover, and durably drain before removal. Until then "add node" is exposed as standby/read
  replicas, explicitly labeled as *not* rebalancing existing shards.
- **L — Licensing gate.** Enterprise-only: create/scale endpoints require a valid license;
  individual/Docker deployments never see the Clusters UI. (Design the license mechanism separately.)

Slices 0–5 are fully deliverable; slice 6's rebalance is the one true dependency on shardlite core.

---

## Decisions / recommendations

- **k8s access via `ureq`+rustls, not kube-rs** — keeps the sync stack intact.
- **Namespace-scoped RBAC** — create/delete on workloads in *one* target namespace, nothing
  cluster-wide.
- **One StatefulSet per cluster; ordinal → node-id.**
- **If live scaling becomes a hard requirement, shardlite shard-move (plan step 11) must be scheduled
  first** — the console cannot fake it.
