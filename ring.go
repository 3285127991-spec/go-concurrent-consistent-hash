// Package consistenthash implements a concurrency-safe consistent hash ring
// with virtual nodes. It depends only on the Go standard library.
package consistenthash

import (
	"errors"
	"hash/fnv"
	"sort"
	"strconv"
	"sync"
)

// Sentinel errors returned by Ring operations.
var (
	ErrNonPositiveReplicas = errors.New("consistenthash: replicas must be greater than zero")
	ErrEmptyNodeName       = errors.New("consistenthash: node name must not be empty")
	ErrNodeAlreadyExists   = errors.New("consistenthash: node already exists")
	ErrNodeNotFound        = errors.New("consistenthash: node not found")
	ErrEmptyRing           = errors.New("consistenthash: ring is empty")
)

// hashFunc maps arbitrary data to a position on the ring.
type hashFunc func(data []byte) uint64

// vnode is a single virtual node placed on the ring.
type vnode struct {
	node    string // name of the owning real node
	replica int    // replica index within the owner
}

// Ring is a consistent hash ring. The zero value is not usable; use New.
//
// The sorted list of occupied positions is maintained incrementally on every
// Add and Remove, so Get never rebuilds or re-sorts the ring. Multiple
// virtual nodes may share one position (hash collisions never drop nodes).
//
// All methods are safe for concurrent use. Add and Remove hold an exclusive
// lock for their whole duration, so other goroutines never observe a
// partially added or partially removed node.
type Ring struct {
	mu       sync.RWMutex
	replicas int
	hash     hashFunc

	positions map[uint64][]vnode  // ring position -> virtual nodes at that position
	sorted    []uint64            // sorted unique positions
	nodes     map[string][]uint64 // real node name -> positions of its virtual nodes
}

// New returns a Ring that maps every real node to replicas virtual nodes.
// replicas must be greater than zero.
func New(replicas int) (*Ring, error) {
	if replicas <= 0 {
		return nil, ErrNonPositiveReplicas
	}
	return &Ring{
		replicas:  replicas,
		hash:      defaultHash,
		positions: make(map[uint64][]vnode),
		nodes:     make(map[string][]uint64),
	}, nil
}

// defaultHash is FNV-1a 64-bit, a stable hash from the standard library:
// the same input always yields the same position, across runs and processes.
func defaultHash(data []byte) uint64 {
	h := fnv.New64a()
	_, _ = h.Write(data) // hash.Hash never returns an error
	return h.Sum64()
}

// vnodeKey derives the hash input for the i-th virtual node of a real node.
func vnodeKey(node string, replica int) string {
	return node + "#" + strconv.Itoa(replica)
}

// Replicas returns the number of virtual nodes each real node owns.
func (r *Ring) Replicas() int {
	return r.replicas
}

// Len returns the number of real nodes currently on the ring.
func (r *Ring) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.nodes)
}

// Members returns the names of all real nodes in stable lexicographical
// order. The returned slice is freshly allocated and isolated from the
// ring's internal state.
func (r *Ring) Members() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	members := make([]string, 0, len(r.nodes))
	for name := range r.nodes {
		members = append(members, name)
	}
	sort.Strings(members)
	return members
}

// Add places node and its replicas virtual nodes on the ring. It returns
// ErrEmptyNodeName for an empty name and ErrNodeAlreadyExists if node is
// already a member.
func (r *Ring) Add(node string) error {
	if node == "" {
		return ErrEmptyNodeName
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.nodes[node]; ok {
		return ErrNodeAlreadyExists
	}
	positions := make([]uint64, 0, r.replicas)
	for i := 0; i < r.replicas; i++ {
		p := r.hash([]byte(vnodeKey(node, i)))
		vns, ok := r.positions[p]
		if !ok {
			r.insertPosition(p)
			positions = append(positions, p)
		} else if !ownsPosition(vns, node) {
			// Position already held by another node (hash collision):
			// keep both, record the position once for this node.
			positions = append(positions, p)
		}
		r.positions[p] = append(vns, vnode{node: node, replica: i})
	}
	r.nodes[node] = positions
	return nil
}

// Remove deletes node and all of its virtual nodes from the ring. It
// returns ErrEmptyNodeName for an empty name and ErrNodeNotFound if node
// is not a member. Positions shared with other nodes (hash collisions)
// keep the other nodes' virtual nodes.
func (r *Ring) Remove(node string) error {
	if node == "" {
		return ErrEmptyNodeName
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	positions, ok := r.nodes[node]
	if !ok {
		return ErrNodeNotFound
	}
	for _, p := range positions {
		vns := r.positions[p]
		kept := make([]vnode, 0, len(vns))
		for _, v := range vns {
			if v.node != node {
				kept = append(kept, v)
			}
		}
		if len(kept) == 0 {
			delete(r.positions, p)
			r.removePosition(p)
		} else {
			r.positions[p] = kept
		}
	}
	delete(r.nodes, node)
	return nil
}

// Get returns the name of the real node responsible for key: the owner of
// the first virtual node met walking clockwise from the key's hash,
// wrapping around to the first position when the end of the ring is
// passed. It returns ErrEmptyRing when the ring has no nodes.
func (r *Ring) Get(key string) (string, error) {
	h := r.hash([]byte(key))
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.sorted) == 0 {
		return "", ErrEmptyRing
	}
	i := sort.Search(len(r.sorted), func(i int) bool { return r.sorted[i] >= h })
	if i == len(r.sorted) {
		i = 0 // wrap around the end of the ring
	}
	return r.positions[r.sorted[i]][0].node, nil
}

// ownsPosition reports whether vns already contains a virtual node of node.
func ownsPosition(vns []vnode, node string) bool {
	for _, v := range vns {
		if v.node == node {
			return true
		}
	}
	return false
}

// insertPosition inserts p into the sorted position list, keeping it
// ordered. It is a no-op if p is already present.
func (r *Ring) insertPosition(p uint64) {
	i := sort.Search(len(r.sorted), func(i int) bool { return r.sorted[i] >= p })
	if i < len(r.sorted) && r.sorted[i] == p {
		return
	}
	r.sorted = append(r.sorted, 0)
	copy(r.sorted[i+1:], r.sorted[i:])
	r.sorted[i] = p
}

// removePosition deletes p from the sorted position list. It is a no-op if
// p is not present.
func (r *Ring) removePosition(p uint64) {
	i := sort.Search(len(r.sorted), func(i int) bool { return r.sorted[i] >= p })
	if i < len(r.sorted) && r.sorted[i] == p {
		r.sorted = append(r.sorted[:i], r.sorted[i+1:]...)
	}
}
