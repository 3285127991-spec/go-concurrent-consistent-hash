package consistenthash

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// hashByVnodeID maps "name#id" to id*100, giving every virtual node a
// distinct, deterministic position. Non-vnode input hashes to 0.
func hashByVnodeID(b []byte) uint32 {
	s := string(b)
	n, err := strconv.Atoi(s[strings.LastIndex(s, "#")+1:])
	if err != nil {
		return 0
	}
	return uint32(n * 100)
}

// totalVNodes reports the number of virtual nodes stored on the ring.
func (r *Ring) totalVNodes() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	n := 0
	for _, bucket := range r.buckets {
		n += len(bucket)
	}
	return n
}

func mustRing(t *testing.T, replicas int) *Ring {
	t.Helper()
	r, err := New(replicas)
	if err != nil {
		t.Fatalf("New(%d): %v", replicas, err)
	}
	return r
}

func TestNewRequiresPositiveReplicas(t *testing.T) {
	for _, replicas := range []int{0, -1, -100} {
		if _, err := New(replicas); err == nil {
			t.Errorf("New(%d) should return an error", replicas)
		}
	}
	if r := mustRing(t, 3); r.Replicas() != 3 {
		t.Fatalf("Replicas() = %d, want 3", r.Replicas())
	}
}

func TestAddAndLen(t *testing.T) {
	r := mustRing(t, 10)
	if !r.Add("alpha") || !r.Add("bravo") {
		t.Fatal("Add of new nodes should return true")
	}
	if got := r.Len(); got != 2 {
		t.Fatalf("Len() = %d, want 2", got)
	}
}

func TestAddDuplicateAndEmptyName(t *testing.T) {
	r := mustRing(t, 10)
	if !r.Add("alpha") {
		t.Fatal("first Add should return true")
	}
	if r.Add("alpha") {
		t.Error("duplicate Add should return false")
	}
	if r.Add("") {
		t.Error("Add of empty name should return false")
	}
	if got := r.Len(); got != 1 {
		t.Fatalf("Len() = %d, want 1", got)
	}
	if got := r.totalVNodes(); got != r.Replicas() {
		t.Fatalf("totalVNodes() = %d, want %d", got, r.Replicas())
	}
}

func TestRemove(t *testing.T) {
	r := mustRing(t, 10)
	r.Add("alpha")
	r.Add("bravo")
	if !r.Remove("alpha") {
		t.Error("Remove of existing node should return true")
	}
	if r.Remove("alpha") {
		t.Error("Remove of already-removed node should return false")
	}
	if r.Remove("ghost") {
		t.Error("Remove of unknown node should return false")
	}
	if got := r.Len(); got != 1 {
		t.Fatalf("Len() = %d, want 1", got)
	}
	if got := r.totalVNodes(); got != r.Replicas() {
		t.Fatalf("totalVNodes() = %d, want %d: removed node's vnodes must be gone", got, r.Replicas())
	}
}

func TestGetEmptyRing(t *testing.T) {
	r := mustRing(t, 10)
	if node, ok := r.Get("key"); ok || node != "" {
		t.Fatalf("Get on empty ring = (%q, %v), want empty result", node, ok)
	}
}

func TestFixedKeyRouting(t *testing.T) {
	r := mustRing(t, 100)
	for _, node := range []string{"node-a", "node-b", "node-c"} {
		r.Add(node)
	}
	want := map[string]string{
		"alpha":   "node-a",
		"bravo":   "node-a",
		"charlie": "node-b",
		"delta":   "node-b",
		"echo":    "node-a",
	}
	for key, w := range want {
		got, ok := r.Get(key)
		if !ok {
			t.Fatalf("Get(%q) returned !ok on a non-empty ring", key)
		}
		if got != w {
			t.Errorf("Get(%q) = %q, want %q", key, got, w)
		}
	}
}

func TestWrapAround(t *testing.T) {
	h := func(b []byte) uint32 {
		switch string(b) {
		case "A#0":
			return 10
		case "B#0":
			return 20
		case "between":
			return 15
		case "past-end":
			return 30
		}
		return 0
	}
	r := newWithHash(1, h)
	r.Add("A")
	r.Add("B")
	if got, _ := r.Get("between"); got != "B" {
		t.Errorf("Get(between) = %q, want B (first position >= 15)", got)
	}
	if got, _ := r.Get("past-end"); got != "A" {
		t.Errorf("Get(past-end) = %q, want A (wrap around the end of the ring)", got)
	}
}

func TestVirtualNodeCount(t *testing.T) {
	r := newWithHash(7, hashByVnodeID)
	r.Add("alpha")
	if got := r.totalVNodes(); got != 7 {
		t.Fatalf("totalVNodes() = %d, want 7", got)
	}
	if got := len(r.positions); got != 7 {
		t.Fatalf("distinct positions = %d, want 7", got)
	}
}

func TestMembersSortedAndIsolated(t *testing.T) {
	r := mustRing(t, 10)
	r.Add("charlie")
	r.Add("alpha")
	r.Add("bravo")
	want := []string{"alpha", "bravo", "charlie"}
	if got := r.Members(); !reflect.DeepEqual(got, want) {
		t.Fatalf("Members() = %v, want %v", got, want)
	}
	got := r.Members()
	got[0] = "mutated"
	if again := r.Members(); !reflect.DeepEqual(again, want) {
		t.Fatalf("Members() after mutating the returned slice = %v, want %v", again, want)
	}
}

func TestReRouteAfterRemove(t *testing.T) {
	h := func(b []byte) uint32 {
		switch string(b) {
		case "A#0":
			return 10
		case "B#0":
			return 20
		case "C#0":
			return 30
		case "key":
			return 25
		}
		return 0
	}
	r := newWithHash(1, h)
	r.Add("A")
	r.Add("B")
	r.Add("C")
	if got, _ := r.Get("key"); got != "C" {
		t.Fatalf("Get(key) = %q, want C", got)
	}
	r.Remove("C")
	if got, _ := r.Get("key"); got != "A" {
		t.Fatalf("Get(key) after removing C = %q, want A (wrap around)", got)
	}
}

func TestHashCollision(t *testing.T) {
	// Every virtual node of both nodes lands on the same position.
	r := newWithHash(3, func(b []byte) uint32 { return 42 })
	r.Add("A")
	r.Add("B")
	if got := r.totalVNodes(); got != 6 {
		t.Fatalf("totalVNodes() = %d, want 6: vnodes must not be lost to collisions", got)
	}
	if got := len(r.positions); got != 1 {
		t.Fatalf("distinct positions = %d, want 1", got)
	}
	if got, _ := r.Get("anything"); got != "A" {
		t.Fatalf("Get = %q, want A (deterministic order within a colliding bucket)", got)
	}
	r.Remove("A")
	if got, _ := r.Get("anything"); got != "B" {
		t.Fatalf("Get after removing A = %q, want B: colliding node must survive", got)
	}
	if got := r.totalVNodes(); got != 3 {
		t.Fatalf("totalVNodes() = %d, want 3", got)
	}
}

func TestConcurrentAccess(t *testing.T) {
	r := mustRing(t, 20)
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 25; i++ {
				name := fmt.Sprintf("node-%d-%d", id, i)
				r.Add(name)
				r.Get("key")
				r.Remove(name)
			}
		}(w)
	}
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				r.Get("key")
				r.Members()
				r.Len()
			}
		}()
	}
	wg.Wait()
	if got := r.Len(); got != 0 {
		t.Fatalf("Len() = %d, want 0 after all nodes are removed", got)
	}
}
