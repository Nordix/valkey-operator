# Design: Rolling Update with Temporary Replicas

## Problem

Without persistence (no PVC), rolling updates lose data in two cases:

1. **Zero replicas:** Restarting the primary destroys all shard data (emptyDir wiped).
2. **Single replica:** If the primary crashes after its replica was already rolled
   (new node ID, not yet synced), the shard has no valid copy of the data.

The current approach relies on an existing synced replica to failover to before
rolling the primary. Without persistence, rolled replicas lose their identity and
must MEET + REPLICATE from scratch — creating a window where no synced replica exists.

## Proposed Solution

Add a configurable rolling update strategy. The new `surge` strategy creates a
**temporary replica** before rolling a shard that:
1. Joins the shard with the **new config** (latest spec)
2. Fully syncs from the current primary
3. Serves as a failover target during the rolling update

This also acts as a **config canary**: if the new configuration or image is
invalid or causes startup failures, the surge replica will never become ready
and the roll will not proceed — protecting the existing primaries from a bad
config or image change.

Once all original nodes in the shard are rolled, the temporary replica is removed.

This provides HA during the entire rolling update regardless of persistence
or replica count.

The strategy is configurable via `spec.rollingUpdate.strategy`. By default
(`auto`), it is enabled for clusters without persistence and disabled for
clusters with PVC — where the existing in-place roll is already fast and
data-safe (pods keep their node ID and data across restarts).

## Requirements

### Functional

1. Before rolling shard N, create a surge ValkeyNode at a fixed high index
   (e.g., `cluster-N-100`) with the desired spec (new config hash).
2. Wait for the surge replica to fully sync with the primary.
3. Proceed with the normal rolling update: roll existing replicas, failover to
   the surge replica, roll the old primary.
4. After all original nodes are updated, remove the surge ValkeyNode
   (scale back to desired replica count).

### Non-Functional

7. The feature should be configurable via `spec.rollingUpdate.strategy`:
   `"auto"` (default), `"surge"`, or `"inPlace"`. Auto selects based on
   whether persistence is configured.
8. In-place behavior (current) remains available for backward compatibility.
9. Temporary replicas must use anti-affinity to land on different nodes than
   the primary (matching existing replica placement).
10. Surge replicas must be cleaned up even if the operator crashes mid-update
    (idempotent reconcile logic: if a shard has more replicas than spec.Replicas
    and no rolling update is in progress, scale down).

### Constraints

11. Must work with both StatefulSet and Deployment workload types.
12. Must work with and without persistence.
13. Should not increase the minimum resource requirements for small clusters.
14. The surge replica's PVC (if persistence is enabled and strategy is forced
    to "surge") should be deleted after removal.

## Sequence Diagram

```
Reconcile: rolling update for shard 0 (replicas=1)
   |
   |── Create surge replica: cluster-0-100
   |   (fixed index, new config hash, joins shard 0)
   |
   |── [main reconcile] MEET surge into cluster
   |
   |── [main reconcile] REPLICATE surge to shard 0 primary
   |
   |── Wait for sync: master_link_status: up (in clusterState)
   |
   |── Roll existing replica (0-1)
   |   (surge replica 0-100 provides HA if primary dies)
   |
   |── Proactive failover: primary (0-0) → surge replica (0-100)
   |   (0-100 has new config + all data)
   |
   |── Roll old primary (0-0)
   |   (restarts with new config, empty data)
   |
   |── [main reconcile] MEET + REPLICATE node 0-0 to surge (0-100)
   |
   |── Wait for node 0-0 to sync with surge
   |
   |── Proactive failover back: surge (0-100) → node 0-0
   |   (0-0 is now primary again with full data)
   |
   |── Remove surge replica (0-100)
   |
   |── Shard 0 complete, advance to shard 1
```

The surge replica uses a fixed index (`cluster-N-100`) to avoid collisions
with regular replicas during simultaneous scaling. It's identified by the
`valkey.io/surge-replica: "true"` label and gets created before and removed
after the shard roll.

## Parallel Rolling with Multiple Extra Replicas

When `extraReplicas: N` (N > 1), the operator can roll up to N shards simultaneously:

```
   |── Create tmp replicas for shards 0, 1, 2 (parallel)
   |── Wait for all to sync
   |── Roll shards 0, 1, 2 in parallel
   |── Clean up tmp replicas
   |── Next batch: shards 3, 4, 5
```

This significantly reduces total rolling update time for large clusters.

## Configuration

```yaml
apiVersion: valkey.io/v1alpha1
kind: ValkeyCluster
spec:
  shards: 6
  replicas: 0
  rollingUpdate:
    strategy: auto        # "auto" (default), "surge", or "inPlace"
```

### Strategy selection

| Strategy  | Behavior |
|-----------|----------|
| `auto`    | Uses `inPlace` when persistence is configured (fast partial resync), `surge` otherwise (avoids data loss) |
| `surge`   | Always creates surge replicas before rolling, regardless of persistence |
| `inPlace` | Current behavior: roll in-place, rely on existing replicas for failover |

`auto` is the default and the recommended setting. With PVC, restarting a pod is
fast (same node ID, partial resync from backlog). Without PVC, a full sync is
required regardless — the surge replica pays this cost upfront before any node is
disrupted, guaranteeing HA throughout.

## Implementation Plan

### Phase 1: Core mechanism (single shard, sequential)

1. Add `spec.rollingUpdate` fields to ValkeyCluster CRD.
2. In `reconcileValkeyNodes`, when a shard needs rolling and strategy is "surge" (or "auto" without PVC):
   a. Create a surge ValkeyNode at index 100 with the new config hash.
   b. MEET + REPLICATE it into the shard.
   c. Wait for sync (`master_link_status: up`).
   d. Roll existing replicas.
   e. Failover primary to the surge replica, roll old primary.
   f. After shard is complete, delete the surge ValkeyNode (scale back).
3. Add cleanup logic: if a shard has nodes beyond `spec.Replicas` and no rolling
   update is in progress, remove excess nodes (crash recovery).

### Phase 2: Parallel shards

4. When `maxParallelShards > 1` (a future config field controlling how many
   shards roll simultaneously), batch shards and roll them concurrently.
5. Each shard in the batch gets its own temporary replica.
6. The operator waits for ALL shards in the batch to complete before starting
   the next batch.

## Implementation Notes (Phase 1)

The following deviations from the original plan were made during implementation:

### Surge node naming

The design proposed using the "next regular index" (`cluster-N-<replicas+1>`).
The implementation uses a **fixed index of 100** (`cluster-N-100`) to avoid
naming collisions during simultaneous replica scaling + config change operations.
This index is guaranteed unreachable since the CRD validates `replicas` with a
maximum of 99. Index 100 is safe regardless of replica count.

The surge is identified by the label `valkey.io/surge-replica: "true"` rather
than by index alone.

### Failover-back before surge removal

The design's step (f) says "delete the surge ValkeyNode (scale back)." In
practice, after the forward failover (primary → surge), the surge owns the
shard's slots. Before removing it, the operator must:

1. Wait for the rolled node 0 to rejoin as a synced replica of the surge.
2. Failover back (surge → node 0) via `CLUSTER FAILOVER`.
3. Only then delete the surge ValkeyNode.

Without this step, removing the surge while it holds slots causes data loss.

### MEET/REPLICATE dependency

The surge cannot introduce itself to the cluster. The main reconcile loop's
MEET and REPLICATE phases handle this. Therefore, `reconcileValkeyNodes` must
NOT block (`return true`) when waiting for a surge to sync. Instead it uses
`continue` to skip unsynced shards, returning `false` so the caller proceeds
to run MEET/REPLICATE. This creates a multi-reconcile progression:

1. Reconcile N: Create surge ValkeyNode → skip shard → return false → MEET
2. Reconcile N+1: Surge MEETed → skip (still in PendingNodes) → REPLICATE
3. Reconcile N+2: Surge replicated and synced in clusterState → proceed to roll

### ClusterState scraping

`clusterState` is scraped at the top of `reconcileValkeyNodes` whenever:
- Any regular node requires a roll, OR
- Any shard has a surge replica (needed for cleanup/failover-back checks)

This ensures the surge cleanup logic can determine whether the surge is still
the primary before attempting removal.

### Sync verification

The sync check (`isSurgeSynced`) requires ALL of:
- `status.PodIP` is set (pod is running)
- `status.Ready` is true (passing health checks)
- The surge appears in `clusterState` within its shard (not in PendingNodes)
- `master_link_status: up` via `GetSyncedReplicas()`

This strict check ensures `proactiveFailover` will find the surge as a valid
failover target in the same `clusterState` snapshot.

### Primary identification during surge roll

After a forward failover, the surge (index 100) becomes the shard primary.
`primaryNodeIndexForShard` normally rejects node indices >= `nodesPerShard`.
During surge rolling, the upper bound is extended to include index 100 so it
recognizes the surge as a valid primary rather than returning -1 and deferring
the roll indefinitely.

### `maxParallelShards` deferred

The `maxParallelShards` field is not included in Phase 1's `RollingUpdateSpec`.
All shards create surges in parallel (single reconcile) but roll sequentially
(one shard advances per reconcile). Phase 2 will add explicit parallelism.

## Open Questions

1. Should the surge replica use the same anti-affinity rules as regular replicas,
   or can it be placed anywhere for faster scheduling?
2. How to handle the case where the surge replica fails to sync (timeout)?
   Fall back to legacy behavior for that shard?
3. Should `maxParallelShards` default to 1 or be derived from cluster size?
