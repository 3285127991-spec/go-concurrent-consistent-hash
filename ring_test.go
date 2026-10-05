package consistenthash

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
)

func mustNew(t *testing.T, replicas int) *Ring {
	t.Helper()
	r, err := New(replicas)
	if err != nil {
		t.Fatalf("New(%d): %v", replicas, err)
	}
	return r
}

func mustAdd(t *testing.T, r *Ring, nodes ...string) {
	t.Helper()
	for _, n := range nodes {
		if err := r.Add(n); err != nil {
			t.Fatalf("Add(%q): %v", n, err)
		}
	}
}

func TestNewRequiresPositiveReplicas(t *testing.T) {
	for _, replicas := range []int{0, -1, -100} {
		if _, err := New(replicas); !errors.Is(err, ErrNonPositiveReplicas) {
			t.Errorf("New(%d) error = %v, want ErrNonPositiveReplicas", replicas, err)
		}
	}
	r := mustNew(t, 7)
	if got := r.Replicas(); got != 7 {
		t.Errorf("Replicas() = %d, want 7", got)
	}
}

func TestAddDuplicateAndEmptyName(t *testing.T) {
	r := mustNew(t, 3)
	mustAdd(t, r, "node-a")
	if err := r.Add("node-a"); !errors.Is(err, ErrNodeAlreadyExists) {
		t.Errorf("duplicate Add error = %v, want ErrNodeAlreadyExists", err)
	}
	if err := r.Add(""); !errors.Is(err, ErrEmptyNodeName) {
		t.Errorf("empty-name Add error = %v, want ErrEmptyNodeName", err)
	}
	if got := r.Len(); got != 1 {
		t.Errorf("Len() = %d, want 1", got)
	}
}

func TestRemoveNode(t *testing.T) {
	r := mustNew(t, 3)
	if err := r.Remove("ghost"); !errors.Is(err, ErrNodeNotFound) {
		t.Errorf("Remove(missing) error = %v, want ErrNodeNotFound", err)
	}
	if err := r.Remove(""); !errors.Is(err, ErrEmptyNodeName) {
		t.Errorf("Remove(empty) error = %v, want ErrEmptyNodeName", err)
	}
	mustAdd(t, r, "node-a", "node-b")
	if err := r.Remove("node-a"); err != nil {
		t.Fatalf("Remove(node-a): %v", err)
	}
	if got := r.Len(); got != 1 {
		t.Errorf("Len() = %d, want 1", got)
	}
	if err := r.Remove("node-a"); !errors.Is(err, ErrNodeNotFound) {
		t.Errorf("second Remove error = %v, want ErrNodeNotFound", err)
	}
	if err := r.Remove("node-b"); err != nil {
		t.Fatalf("Remove(node-b): %v", err)
	}
	if len(r.sorted) != 0 || len(r.positions) != 0 {
		t.Errorf("ring not empty after removing all nodes: sorted=%d positions=%d",
			len(r.sorted), len(r.positions))
	}
}

func TestGetEmptyRing(t *testing.T) {
	r := mustNew(t, 3)
	if _, err := r.Get("key"); !errors.Is(err, ErrEmptyRing) {
		t.Errorf("Get on empty ring error = %v, want ErrEmptyRing", err)
	}
}

// stubHash maps exact strings to ring positions, giving tests full control
// over where virtual nodes and keys land.
func stubHash(positions map[string]uint64) hashFunc {
	return func(data []byte) uint64 {
		if p, ok := positions[string(data)]; ok {
			return p
		}
		return 0
	}
}

func TestGetFixedKeyRouting(t *testing.T) {
	r := mustNew(t, 2)
	r.hash = stubHash(map[string]uint64{
		"node-a#0": 100, "node-a#1": 400,
		"node-b#0": 200, "node-b#1": 500,
		"key-1": 150, // between 100 and 200 -> node-b
		"key-2": 450, // between 400 and 500 -> node-b
		"key-3": 250, // between 200 and 400 -> node-a
	})
	mustAdd(t, r, "node-a", "node-b")

	cases := map[string]string{"key-1": "node-b", "key-2": "node-b", "key-3": "node-a"}
	for key, want := range cases {
		got, err := r.Get(key)
		if err != nil {
			t.Fatalf("Get(%q): %v", key, err)
		}
		if got != want {
			t.Errorf("Get(%q) = %q, want %q", key, got, want)
		}
		again, _ := r.Get(key)
		if again != got {
			t.Errorf("Get(%q) unstable: %q then %q", key, got, again)
		}
	}
}

func TestGetWrapsAroundRingEnd(t *testing.T) {
	r := mustNew(t, 1)
	r.hash = stubHash(map[string]uint64{
		"node-a#0": 10,
		"node-b#0": 20,
		"key":      30, // past the last position -> wraps to 10
	})
	mustAdd(t, r, "node-a", "node-b")
	if got, err := r.Get("key"); err != nil || got != "node-a" {
		t.Errorf("Get(key) = %q, %v; want node-a (wrap around ring end)", got, err)
	}
}

func TestVirtualNodeCount(t *testing.T) {
	const replicas = 25
	r := mustNew(t, replicas)
	mustAdd(t, r, "node-a")

	if got := len(r.sorted); got != replicas {
		t.Fatalf("occupied positions = %d, want %d", got, replicas)
	}
	seen := make(map[int]bool)
	for _, p := range r.sorted {
		vns := r.positions[p]
		if len(vns) != 1 || vns[0].node != "node-a" {
			t.Fatalf("position %d holds %+v, want one vnode of node-a", p, vns)
		}
		seen[vns[0].replica] = true
	}
	for i := 0; i < replicas; i++ {
		if !seen[i] {
			t.Errorf("replica %d missing from ring", i)
		}
	}
}

func TestMembersSortedAndIsolated(t *testing.T) {
	r := mustNew(t, 4)
	mustAdd(t, r, "charlie", "alpha", "bravo")

	want := []string{"alpha", "bravo", "charlie"}
	got := r.Members()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Members() = %v, want %v", got, want)
	}
	got[0] = "mutated"
	if again := r.Members(); !reflect.DeepEqual(again, want) {
		t.Errorf("Members() after mutating returned slice = %v, want %v", again, want)
	}
}

func TestRemoveReroutesKeys(t *testing.T) {
	r := mustNew(t, 1)
	r.hash = stubHash(map[string]uint64{
		"node-a#0": 10,
		"node-b#0": 20,
		"key":      15, // -> node-b while present, node-a after removal
	})
	mustAdd(t, r, "node-a", "node-b")
	if got, _ := r.Get("key"); got != "node-b" {
		t.Fatalf("Get(key) = %q, want node-b", got)
	}
	if err := r.Remove("node-b"); err != nil {
		t.Fatalf("Remove(node-b): %v", err)
	}
	if got, _ := r.Get("key"); got != "node-a" {
		t.Errorf("Get(key) after removal = %q, want node-a", got)
	}
}

func TestHashCollisionHandling(t *testing.T) {
	r := mustNew(t, 3)
	r.hash = func([]byte) uint64 { return 42 } // every vnode and key collides
	mustAdd(t, r, "node-a", "node-b")

	if got := len(r.sorted); got != 1 {
		t.Fatalf("occupied positions = %d, want 1 shared position", got)
	}
	if got := len(r.positions[42]); got != 6 {
		t.Fatalf("vnodes at shared position = %d, want 6 (no silent loss)", got)
	}
	if got := r.Len(); got != 2 {
		t.Fatalf("Len() = %d, want 2", got)
	}
	if got, _ := r.Get("anything"); got != "node-a" {
		t.Errorf("Get = %q, want node-a (first vnode at the position)", got)
	}

	// Removing one node must keep the other node's vnodes at the position.
	if err := r.Remove("node-a"); err != nil {
		t.Fatalf("Remove(node-a): %v", err)
	}
	if got := len(r.positions[42]); got != 3 {
		t.Errorf("vnodes at shared position after removal = %d, want 3", got)
	}
	if got, _ := r.Get("anything"); got != "node-b" {
		t.Errorf("Get after removal = %q, want node-b", got)
	}
}

func TestConcurrentAccess(t *testing.T) {
	r := mustNew(t, 8)
	const nodes = 16

	var wg sync.WaitGroup
	readers := func(stop <-chan struct{}) {
		for i := 0; i < 4; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; ; j++ {
					select {
					case <-stop:
						return
					default:
					}
					// May briefly see an empty or partial ring; results
					// must always be a definitive value or error.
					if _, err := r.Get(fmt.Sprintf("key-%d", j)); err != nil && !errors.Is(err, ErrEmptyRing) {
						t.Errorf("Get: unexpected error %v", err)
					}
					_ = r.Members()
					_ = r.Len()
				}
			}()
		}
	}

	stop := make(chan struct{})
	readers(stop)
	for i := 0; i < nodes; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := r.Add(fmt.Sprintf("node-%02d", i)); err != nil {
				t.Errorf("Add: %v", err)
			}
		}(i)
	}
	close(stop)
	wg.Wait()

	if got := r.Len(); got != nodes {
		t.Fatalf("Len() = %d, want %d after concurrent adds", got, nodes)
	}
	if got := len(r.Members()); got != nodes {
		t.Fatalf("len(Members()) = %d, want %d", got, nodes)
	}
	first, err := r.Get("stable-key")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	for i := 0; i < 5; i++ {
		if got, _ := r.Get("stable-key"); got != first {
			t.Fatalf("Get(stable-key) unstable: %q then %q", first, got)
		}
	}

	stop = make(chan struct{})
	readers(stop)
	for i := 0; i < nodes/2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := r.Remove(fmt.Sprintf("node-%02d", i)); err != nil {
				t.Errorf("Remove: %v", err)
			}
		}(i)
	}
	close(stop)
	wg.Wait()

	if got := r.Len(); got != nodes/2 {
		t.Errorf("Len() = %d, want %d after concurrent removes", got, nodes/2)
	}
}
