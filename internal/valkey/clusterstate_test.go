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

package valkey

import (
	"reflect"
	"testing"
)

func TestParseSlotsRange(t *testing.T) {
	// Slot range
	slots, err := parseSlotsRange("0-16383")
	if err != nil {
		t.Errorf("Expected not expected, got %v", err)
	}
	expect := SlotsRange{0, 16383}
	if slots != expect {
		t.Errorf("Expected %v, got %v", expect, slots)
	}

	// Single slot range
	slots, err = parseSlotsRange("5")
	if err != nil {
		t.Errorf("Expected not expected, got %v", err)
	}
	expect = SlotsRange{5, 5}
	if slots != expect {
		t.Errorf("Expected %v, got %v", expect, slots)
	}
}

func TestSubtractSlotsRange(t *testing.T) {
	base := SlotsRange{0, 16383}
	remove := SlotsRange{10, 16380}
	expect := []SlotsRange{SlotsRange{0, 9}, SlotsRange{16381, 16383}}
	result := subtractSlotsRange(base, remove)
	if !reflect.DeepEqual(result, expect) {
		t.Errorf("Expected %v, got %v", expect, result)
	}

	base = SlotsRange{0, 10}
	remove = SlotsRange{5, 10}
	expect = []SlotsRange{SlotsRange{0, 4}}
	result = subtractSlotsRange(base, remove)
	if !reflect.DeepEqual(result, expect) {
		t.Errorf("Expected %v, got %v", expect, result)
	}

	base = SlotsRange{0, 10}
	remove = SlotsRange{0, 9}
	expect = []SlotsRange{SlotsRange{10, 10}}
	result = subtractSlotsRange(base, remove)
	if !reflect.DeepEqual(result, expect) {
		t.Errorf("Expected %v, got %v", expect, result)
	}

	base = SlotsRange{0, 10}
	remove = SlotsRange{0, 10}
	result = subtractSlotsRange(base, remove)
	if result != nil {
		t.Errorf("Expected nil, got %v", result)
	}
}

func TestGetUnassignedSlots(t *testing.T) {
	// A shard with no unassigned slots
	cluster := ClusterState{
		Shards: []*ShardState{
			&ShardState{
				Slots: []SlotsRange{SlotsRange{0, 16383}},
			},
		},
	}
	result := cluster.GetUnassignedSlots()
	if len(result) != 0 {
		t.Errorf("Expected empty array, got %v", result)
	}

	// A single shard with the unassigned slot 0
	cluster = ClusterState{
		Shards: []*ShardState{
			&ShardState{
				Slots: []SlotsRange{SlotsRange{1, 16383}},
			},
		},
	}
	result = cluster.GetUnassignedSlots()
	expect := []SlotsRange{SlotsRange{0, 0}}
	if !reflect.DeepEqual(result, expect) {
		t.Errorf("Expected %v, got %v", expect, result)
	}

	// Three shards with unassigned slots
	cluster = ClusterState{
		Shards: []*ShardState{
			&ShardState{
				Slots: []SlotsRange{SlotsRange{100, 200}, SlotsRange{300, 400}},
			},
			&ShardState{
				Slots: []SlotsRange{SlotsRange{700, 800}},
			},
			&ShardState{
				Slots: []SlotsRange{SlotsRange{500, 600}},
			},
		},
	}
	result = cluster.GetUnassignedSlots()
	expect = []SlotsRange{SlotsRange{0, 99}, SlotsRange{201, 299},
		SlotsRange{401, 499}, SlotsRange{601, 699}, SlotsRange{801, 16383}}
	if !reflect.DeepEqual(result, expect) {
		t.Errorf("Expected %v, got %v", expect, result)
	}
}
