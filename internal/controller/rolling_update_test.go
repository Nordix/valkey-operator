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
	"testing"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	valkeyiov1alpha1 "valkey.io/valkey-operator/api/v1alpha1"
	"valkey.io/valkey-operator/internal/valkey"
)

func TestEffectiveRollingStrategy(t *testing.T) {
	tests := []struct {
		name     string
		cluster  *valkeyiov1alpha1.ValkeyCluster
		expected valkeyiov1alpha1.RollingUpdateStrategy
	}{
		{
			name: "auto with no persistence returns surge",
			cluster: &valkeyiov1alpha1.ValkeyCluster{
				Spec: valkeyiov1alpha1.ValkeyClusterSpec{Replicas: 1},
			},
			expected: valkeyiov1alpha1.RollingUpdateStrategySurge,
		},
		{
			name: "auto with persistence returns inPlace",
			cluster: &valkeyiov1alpha1.ValkeyCluster{
				Spec: valkeyiov1alpha1.ValkeyClusterSpec{
					Replicas:    1,
					Persistence: &valkeyiov1alpha1.PersistenceSpec{},
				},
			},
			expected: valkeyiov1alpha1.RollingUpdateStrategyInPlace,
		},
		{
			name: "explicit surge overrides auto",
			cluster: &valkeyiov1alpha1.ValkeyCluster{
				Spec: valkeyiov1alpha1.ValkeyClusterSpec{
					Persistence: &valkeyiov1alpha1.PersistenceSpec{},
					RollingUpdate: &valkeyiov1alpha1.RollingUpdateSpec{
						Strategy: valkeyiov1alpha1.RollingUpdateStrategySurge,
					},
				},
			},
			expected: valkeyiov1alpha1.RollingUpdateStrategySurge,
		},
		{
			name: "explicit inPlace overrides auto",
			cluster: &valkeyiov1alpha1.ValkeyCluster{
				Spec: valkeyiov1alpha1.ValkeyClusterSpec{
					RollingUpdate: &valkeyiov1alpha1.RollingUpdateSpec{
						Strategy: valkeyiov1alpha1.RollingUpdateStrategyInPlace,
					},
				},
			},
			expected: valkeyiov1alpha1.RollingUpdateStrategyInPlace,
		},
		{
			name: "nil rollingUpdate with no persistence returns surge",
			cluster: &valkeyiov1alpha1.ValkeyCluster{
				Spec: valkeyiov1alpha1.ValkeyClusterSpec{},
			},
			expected: valkeyiov1alpha1.RollingUpdateStrategySurge,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, effectiveRollingStrategy(tt.cluster))
		})
	}
}

func TestSurgeNodeIndex(t *testing.T) {
	cluster := &valkeyiov1alpha1.ValkeyCluster{
		Spec: valkeyiov1alpha1.ValkeyClusterSpec{Replicas: 2},
	}
	assert.Equal(t, 100, surgeNodeIndex(cluster))
}

func TestShardNeedsRoll(t *testing.T) {
	cluster := &valkeyiov1alpha1.ValkeyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
		Spec: valkeyiov1alpha1.ValkeyClusterSpec{
			Shards:   3,
			Replicas: 1,
		},
	}

	t.Run("returns true when config hash differs", func(t *testing.T) {
		nodes := &valkeyiov1alpha1.ValkeyNodeList{
			Items: []valkeyiov1alpha1.ValkeyNode{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "test-0-0", Namespace: "default", Labels: map[string]string{LabelShardIndex: "0", LabelNodeIndex: "0", LabelCluster: "test"}},
					Spec:       valkeyiov1alpha1.ValkeyNodeSpec{ServerConfigHash: "old-hash"},
					Status:     valkeyiov1alpha1.ValkeyNodeStatus{PodIP: "10.0.0.1"},
				},
				{
					ObjectMeta: metav1.ObjectMeta{Name: "test-0-1", Namespace: "default", Labels: map[string]string{LabelShardIndex: "0", LabelNodeIndex: "1", LabelCluster: "test"}},
					Spec:       valkeyiov1alpha1.ValkeyNodeSpec{ServerConfigHash: "new-hash"},
					Status:     valkeyiov1alpha1.ValkeyNodeStatus{PodIP: "10.0.0.2"},
				},
			},
		}
		assert.True(t, shardNeedsRoll(cluster, 0, nodes, "new-hash"))
	})

	t.Run("returns false when all nodes match", func(t *testing.T) {
		nodes := &valkeyiov1alpha1.ValkeyNodeList{
			Items: []valkeyiov1alpha1.ValkeyNode{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "test-0-0", Namespace: "default", Labels: map[string]string{LabelShardIndex: "0", LabelNodeIndex: "0", LabelCluster: "test"}},
					Spec:       buildClusterValkeyNode(cluster, 0, 0).Spec,
					Status:     valkeyiov1alpha1.ValkeyNodeStatus{PodIP: "10.0.0.1"},
				},
				{
					ObjectMeta: metav1.ObjectMeta{Name: "test-0-1", Namespace: "default", Labels: map[string]string{LabelShardIndex: "0", LabelNodeIndex: "1", LabelCluster: "test"}},
					Spec:       buildClusterValkeyNode(cluster, 0, 1).Spec,
					Status:     valkeyiov1alpha1.ValkeyNodeStatus{PodIP: "10.0.0.2"},
				},
			},
		}
		nodes.Items[0].Spec.ServerConfigHash = "new-hash"
		nodes.Items[1].Spec.ServerConfigHash = "new-hash"
		assert.False(t, shardNeedsRoll(cluster, 0, nodes, "new-hash"))
	})
}

func TestIsSurgeReplica(t *testing.T) {
	t.Run("true with label", func(t *testing.T) {
		node := &valkeyiov1alpha1.ValkeyNode{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{LabelSurgeReplica: "true"}},
		}
		assert.True(t, isSurgeReplica(node))
	})

	t.Run("false without label", func(t *testing.T) {
		node := &valkeyiov1alpha1.ValkeyNode{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{}},
		}
		assert.False(t, isSurgeReplica(node))
	})
}

func TestShardHasSurgeReplica(t *testing.T) {
	cluster := &valkeyiov1alpha1.ValkeyCluster{
		Spec: valkeyiov1alpha1.ValkeyClusterSpec{Replicas: 1},
	}

	t.Run("true when surge exists", func(t *testing.T) {
		nodes := &valkeyiov1alpha1.ValkeyNodeList{
			Items: []valkeyiov1alpha1.ValkeyNode{
				{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{LabelShardIndex: "0", LabelNodeIndex: "0"}}},
				{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{LabelShardIndex: "0", LabelNodeIndex: "1"}}},
				{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{LabelShardIndex: "0", LabelNodeIndex: "100", LabelSurgeReplica: "true"}}},
			},
		}
		assert.True(t, shardHasSurgeReplica(cluster, 0, nodes))
	})

	t.Run("false when no surge", func(t *testing.T) {
		nodes := &valkeyiov1alpha1.ValkeyNodeList{
			Items: []valkeyiov1alpha1.ValkeyNode{
				{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{LabelShardIndex: "0", LabelNodeIndex: "0"}}},
				{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{LabelShardIndex: "0", LabelNodeIndex: "1"}}},
			},
		}
		assert.False(t, shardHasSurgeReplica(cluster, 0, nodes))
	})
}

func TestIsSurgeSynced(t *testing.T) {
	cluster := &valkeyiov1alpha1.ValkeyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
		Spec:       valkeyiov1alpha1.ValkeyClusterSpec{Replicas: 1},
	}

	t.Run("returns false with nil cluster state and no role", func(t *testing.T) {
		r := &ValkeyClusterReconciler{}
		nodes := &valkeyiov1alpha1.ValkeyNodeList{
			Items: []valkeyiov1alpha1.ValkeyNode{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "test-0-100"},
					Status:     valkeyiov1alpha1.ValkeyNodeStatus{PodIP: "10.0.0.3", Ready: true},
				},
			},
		}
		assert.False(t, r.isSurgeSynced(context.TODO(), cluster, 0, nil, nodes))
	})

	t.Run("returns false when surge has no pod IP", func(t *testing.T) {
		r := &ValkeyClusterReconciler{}
		nodes := &valkeyiov1alpha1.ValkeyNodeList{
			Items: []valkeyiov1alpha1.ValkeyNode{
				{ObjectMeta: metav1.ObjectMeta{Name: "test-0-100"}},
			},
		}
		assert.False(t, r.isSurgeSynced(context.TODO(), cluster, 0, nil, nodes))
	})

	t.Run("returns true when surge is synced in cluster state", func(t *testing.T) {
		r := &ValkeyClusterReconciler{}
		state := &valkey.ClusterState{
			Shards: []*valkey.ShardState{
				{
					Id:        "shard-0",
					PrimaryId: "node-1",
					Nodes: []*valkey.NodeState{
						{Address: "10.0.0.1", Id: "node-1", Flags: []string{"master"}},
						{Address: "10.0.0.3", Id: "node-3", Flags: []string{"slave"}, Info: map[string]string{"master_link_status": "up"}},
					},
				},
			},
		}
		nodes := &valkeyiov1alpha1.ValkeyNodeList{
			Items: []valkeyiov1alpha1.ValkeyNode{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "test-0-100"},
					Status:     valkeyiov1alpha1.ValkeyNodeStatus{PodIP: "10.0.0.3", Ready: true},
				},
			},
		}
		assert.True(t, r.isSurgeSynced(context.TODO(), cluster, 0, state, nodes))
	})

	t.Run("returns false when surge is in shard but not synced", func(t *testing.T) {
		r := &ValkeyClusterReconciler{}
		state := &valkey.ClusterState{
			Shards: []*valkey.ShardState{
				{
					Id:        "shard-0",
					PrimaryId: "node-1",
					Nodes: []*valkey.NodeState{
						{Address: "10.0.0.1", Id: "node-1", Flags: []string{"master"}},
						{Address: "10.0.0.3", Id: "node-3", Flags: []string{"slave"}, Info: map[string]string{"master_link_status": "down"}},
					},
				},
			},
		}
		nodes := &valkeyiov1alpha1.ValkeyNodeList{
			Items: []valkeyiov1alpha1.ValkeyNode{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "test-0-100"},
					Status:     valkeyiov1alpha1.ValkeyNodeStatus{PodIP: "10.0.0.3", Ready: true},
				},
			},
		}
		assert.False(t, r.isSurgeSynced(context.TODO(), cluster, 0, state, nodes))
	})

	t.Run("returns false when not in cluster state shard", func(t *testing.T) {
		r := &ValkeyClusterReconciler{}
		state := &valkey.ClusterState{Shards: []*valkey.ShardState{}}
		nodes := &valkeyiov1alpha1.ValkeyNodeList{
			Items: []valkeyiov1alpha1.ValkeyNode{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "test-0-100"},
					Status:     valkeyiov1alpha1.ValkeyNodeStatus{PodIP: "10.0.0.3", Ready: true, Role: "replica"},
				},
			},
		}
		assert.False(t, r.isSurgeSynced(context.TODO(), cluster, 0, state, nodes))
	})
}
