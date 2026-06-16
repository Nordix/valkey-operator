/*
Copyright 2025 Valkey Contributors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"fmt"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	valkeyiov1alpha1 "valkey.io/valkey-operator/api/v1alpha1"
	"valkey.io/valkey-operator/internal/valkey"
)

// LabelSurgeReplica marks a ValkeyNode as a temporary surge replica for surge rolling updates.
const LabelSurgeReplica = "valkey.io/surge-replica"

// effectiveRollingStrategy determines the rolling update strategy for the cluster.
func effectiveRollingStrategy(cluster *valkeyiov1alpha1.ValkeyCluster) valkeyiov1alpha1.RollingUpdateStrategy {
	if cluster.Spec.RollingUpdate != nil && cluster.Spec.RollingUpdate.Strategy != "" &&
		cluster.Spec.RollingUpdate.Strategy != valkeyiov1alpha1.RollingUpdateStrategyAuto {
		return cluster.Spec.RollingUpdate.Strategy
	}
	if cluster.Spec.Persistence == nil {
		return valkeyiov1alpha1.RollingUpdateStrategySurge
	}
	return valkeyiov1alpha1.RollingUpdateStrategyInPlace
}

// surgeNodeIndex returns the node index for the surge replica within a shard.
// Uses index 100 which is guaranteed unreachable (replicas max is 99).
func surgeNodeIndex(_ *valkeyiov1alpha1.ValkeyCluster) int {
	return 100
}

// shardNeedsRoll returns true if any node in the shard has a spec that differs from desired.
func shardNeedsRoll(cluster *valkeyiov1alpha1.ValkeyCluster, shardIndex int, nodes *valkeyiov1alpha1.ValkeyNodeList, configHash string) bool {
	nodesPerShard := 1 + int(cluster.Spec.Replicas)
	byName := make(map[string]*valkeyiov1alpha1.ValkeyNode, len(nodes.Items))
	for i := range nodes.Items {
		byName[nodes.Items[i].Name] = &nodes.Items[i]
	}
	for nodeIndex := range nodesPerShard {
		desired := buildClusterValkeyNode(cluster, shardIndex, nodeIndex)
		desired.Spec.ServerConfigHash = configHash
		if current, ok := byName[desired.Name]; ok && nodeRequiresRoll(current, desired) {
			return true
		}
	}
	return false
}

// ensureSurgeSynced creates the surge replica if needed and checks if it's synced.
// Returns true if the surge is synced and the shard is ready to roll, false if
// the shard should be skipped this reconcile.
func (r *ValkeyClusterReconciler) ensureSurgeSynced(ctx context.Context, cluster *valkeyiov1alpha1.ValkeyCluster, shardIndex int, nodes *valkeyiov1alpha1.ValkeyNodeList, clusterState *valkey.ClusterState, configHash string) bool {
	exists, err := r.surgeReplicaExists(ctx, cluster, shardIndex)
	if err != nil {
		return false
	}
	if !exists {
		_ = r.createSurgeReplica(ctx, cluster, shardIndex, configHash)
		return false
	}
	return r.isSurgeSynced(ctx, cluster, shardIndex, clusterState, nodes)
}

// surgeReplicaExists checks if a surge replica ValkeyNode exists for the given shard.
func (r *ValkeyClusterReconciler) surgeReplicaExists(ctx context.Context, cluster *valkeyiov1alpha1.ValkeyCluster, shardIndex int) (bool, error) {
	nodeIndex := surgeNodeIndex(cluster)
	name := valkeyNodeName(cluster.Name, shardIndex, nodeIndex)
	node := &valkeyiov1alpha1.ValkeyNode{}
	err := r.Get(ctx, client.ObjectKey{Name: name, Namespace: cluster.Namespace}, node)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// createSurgeReplica creates a temporary surge ValkeyNode for the shard with the new config.
func (r *ValkeyClusterReconciler) createSurgeReplica(ctx context.Context, cluster *valkeyiov1alpha1.ValkeyCluster, shardIndex int, configHash string) error {
	log := logf.FromContext(ctx)
	nodeIndex := surgeNodeIndex(cluster)
	desired := buildClusterValkeyNode(cluster, shardIndex, nodeIndex)
	desired.Spec.ServerConfigHash = configHash
	desired.Labels[LabelSurgeReplica] = "true"

	node := &valkeyiov1alpha1.ValkeyNode{
		ObjectMeta: metav1.ObjectMeta{
			Name:      desired.Name,
			Namespace: desired.Namespace,
		},
	}

	result, err := controllerutil.CreateOrUpdate(ctx, r.Client, node, func() error {
		node.Labels = desired.Labels
		node.Spec = desired.Spec
		return controllerutil.SetControllerReference(cluster, node, r.Scheme)
	})
	if err != nil {
		return fmt.Errorf("create surge replica %s: %w", desired.Name, err)
	}
	if result == controllerutil.OperationResultCreated {
		log.Info("created surge replica", "name", desired.Name, "shard", shardIndex)
		r.Recorder.Eventf(cluster, node, corev1.EventTypeNormal, "SurgeReplicaCreated", "SurgeRollingUpdate",
			"Created surge replica %s for shard %d", desired.Name, shardIndex)
	}
	return nil
}

// isSurgeSynced checks if the surge replica for a shard has synced with the primary.
func (r *ValkeyClusterReconciler) isSurgeSynced(_ context.Context, cluster *valkeyiov1alpha1.ValkeyCluster, shardIndex int, clusterState *valkey.ClusterState, nodes *valkeyiov1alpha1.ValkeyNodeList) bool {
	if clusterState == nil {
		return false
	}
	nodeIndex := surgeNodeIndex(cluster)
	name := valkeyNodeName(cluster.Name, shardIndex, nodeIndex)

	// Find the surge node in the nodes list.
	var surgeNode *valkeyiov1alpha1.ValkeyNode
	for i := range nodes.Items {
		if nodes.Items[i].Name == name {
			surgeNode = &nodes.Items[i]
			break
		}
	}
	if surgeNode == nil || surgeNode.Status.PodIP == "" || !surgeNode.Status.Ready {
		return false
	}

	// Verify via live cluster state that the surge is a synced replica
	// (master_link_status: up). This guarantees it has a full data copy
	// and proactiveFailover will find it as a valid failover target.
	shard := clusterState.FindShardForAddress(surgeNode.Status.PodIP)
	if shard == nil {
		return false
	}
	for _, replica := range shard.GetSyncedReplicas() {
		if replica.Address == surgeNode.Status.PodIP {
			return true
		}
	}
	return false
}

// removeSurgeReplica deletes the surge ValkeyNode for the given shard.
func (r *ValkeyClusterReconciler) removeSurgeReplica(ctx context.Context, cluster *valkeyiov1alpha1.ValkeyCluster, shardIndex int) error {
	log := logf.FromContext(ctx)
	nodeIndex := surgeNodeIndex(cluster)
	name := valkeyNodeName(cluster.Name, shardIndex, nodeIndex)
	node := &valkeyiov1alpha1.ValkeyNode{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: cluster.Namespace,
		},
	}
	if err := r.Delete(ctx, node); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("delete surge replica %s: %w", name, err)
	}
	log.Info("removed surge replica", "name", name, "shard", shardIndex)
	r.Recorder.Eventf(cluster, nil, corev1.EventTypeNormal, "SurgeReplicaRemoved", "SurgeRollingUpdate",
		"Removed surge replica %s for shard %d", name, shardIndex)
	return nil
}

// isSurgeReplica returns true if the ValkeyNode is labelled as a surge replica.
func isSurgeReplica(node *valkeyiov1alpha1.ValkeyNode) bool {
	return node.Labels[LabelSurgeReplica] == "true"
}

// cleanupSurgeReplica handles failover-back and removal of the surge replica
// after a shard is fully rolled. Returns (true, nil) if an action was taken
// and the caller should requeue, (false, nil) if the surge is still primary
// and waiting for node 0 to sync (caller should skip this shard).
func (r *ValkeyClusterReconciler) cleanupSurgeReplica(ctx context.Context, cluster *valkeyiov1alpha1.ValkeyCluster, shardIndex int, nodes *valkeyiov1alpha1.ValkeyNodeList, clusterState *valkey.ClusterState) (bool, error) {
	log := logf.FromContext(ctx)
	surgeIdx := surgeNodeIndex(cluster)
	surgeName := valkeyNodeName(cluster.Name, shardIndex, surgeIdx)

	var surgeIP string
	for i := range nodes.Items {
		if nodes.Items[i].Name == surgeName {
			surgeIP = nodes.Items[i].Status.PodIP
			break
		}
	}

	if surgeIP != "" && clusterState != nil {
		shard, replicas := findFailoverShard(clusterState, surgeIP)
		if shard != nil {
			// Surge is still the primary with synced replicas — failover back.
			if err := proactiveFailover(ctx, r.Recorder, cluster, shard, replicas); err != nil {
				log.Info("failover from surge did not complete, will retry", "shard", shardIndex, "err", err)
			}
			return true, nil
		}
		// If surge is still primary (no synced replicas yet), wait for
		// node 0 to be MEETed/REPLICATEd back by the main reconcile.
		shardState := clusterState.FindShardForAddress(surgeIP)
		if shardState != nil && shardState.GetPrimaryNode() != nil &&
			shardState.GetPrimaryNode().Address == surgeIP {
			return false, nil
		}
	}

	// Surge is no longer primary — safe to remove.
	if err := r.removeSurgeReplica(ctx, cluster, shardIndex); err != nil {
		return false, err
	}
	return true, nil
}

// shardHasSurgeReplica checks if any node for the shard is a surge replica.
func shardHasSurgeReplica(cluster *valkeyiov1alpha1.ValkeyCluster, shardIndex int, nodes *valkeyiov1alpha1.ValkeyNodeList) bool {
	si := strconv.Itoa(shardIndex)
	nodesPerShard := 1 + int(cluster.Spec.Replicas)
	for i := range nodes.Items {
		n := &nodes.Items[i]
		if n.Labels[LabelShardIndex] != si {
			continue
		}
		nodeIdx, err := strconv.Atoi(n.Labels[LabelNodeIndex])
		if err != nil {
			continue
		}
		if nodeIdx >= nodesPerShard && isSurgeReplica(n) {
			return true
		}
	}
	return false
}
