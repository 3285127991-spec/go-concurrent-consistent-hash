// Package consistenthash implements a concurrency-safe consistent hash ring.
//
// Each real node is mapped to a fixed number of virtual nodes placed on a
// sorted hash ring. Keys are routed clockwise to the first virtual node at
// or after the key's hash, wrapping around the end of the ring. The ring
// structure is maintained incrementally on Add/Remove, so Get never
// rebuilds or re-sorts the ring.
package consistenthash

import (
	"fmt"
	"hash/crc32"
	"sort"
	"strconv"
	"sync"
)

// hashFunc maps arbitrary data to a position on the ring.
type hashFunc func(data []byte) uint32

// vnode is a single virtual node on the ring.
type vnode struct {
	pos  uint32 // position on the ring
	node string // name of the owning real node
	id   int    // virtual node index in [0, replicas)
}

// Ring is a concurrency-safe consistent hash ring. The zero value is not
// usable; create a Ring with New.
type Ring struct {
	mu        sync.RWMutex
	replicas  int
	hash      hashFunc
	positions []uint32            // sorted unique virtual node positions
	buckets   map[uint32][]vnode  // position -> vnodes, sorted by (node, id)
	members   map[string]struct{} // real node names
}

// New creates a Ring that maps every real node to replicas virtual nodes.
// It returns an error if replicas is not greater than zero.
func New(replicas int) (*Ring, error) {
	if replicas <= 0 {
		return nil, fmt.Errorf("consistenthash: replicas must be > 0, got %d", replicas)
	}
	return &Ring{
		replicas: replicas,
		hash:     crc32.ChecksumIEEE,
		buckets:  make(map[uint32][]vnode),
		members:  make(map[string]struct{}),
	}, nil
}

// newWithHash is New with a custom hash function. It is used by tests to
// force specific ring layouts and hash collisions.
func newWithHash(replicas int, h hashFunc) *Ring {
	r, err := New(replicas)
	if err != nil {
		panic(err)
	}
	r.hash = h
	return r
}

// Replicas returns the number of virtual nodes each real node owns.
func (r *Ring) Replicas() int {
	return r.replicas
}

// Len returns the number of real nodes currently on the ring.
func (r *Ring) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.members)
}

// Add places node's virtual nodes on the ring. It returns false, without
// changing the ring, if node is empty or already present.
func (r *Ring) Add(node string) bool {
	if node == "" {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.members[node]; ok {
		return false
	}
	for i := 0; i < r.replicas; i++ {
		r.insertVNode(vnode{pos: r.hash(vnodeKey(node, i)), node: node, id: i})
	}
	r.members[node] = struct{}{}
	return true
}

// Remove deletes all of node's virtual nodes from the ring. It returns
// false, without changing the ring, if node is not present.
func (r *Ring) Remove(node string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.members[node]; !ok {
		return false
	}
	for i := 0; i < r.replicas; i++ {
		r.removeVNode(vnode{pos: r.hash(vnodeKey(node, i)), node: node, id: i})
	}
	delete(r.members, node)
	return true
}

// Get routes key clockwise to the first virtual node at or after the key's
// hash position, wrapping around the end of the ring, and returns the
// owning real node. It returns ("", false) when the ring is empty.
func (r *Ring) Get(key string) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.positions) == 0 {
		return "", false
	}
	h := r.hash([]byte(key))
	i := sort.Search(len(r.positions), func(i int) bool { return r.positions[i] >= h })
	if i == len(r.positions) {
		i = 0 // past the end of the ring: wrap to the first position
	}
	return r.buckets[r.positions[i]][0].node, true
}

// Members returns the names of all real nodes in sorted order. The
// returned slice is a copy and is safe for the caller to modify.
func (r *Ring) Members() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.members))
	for m := range r.members {
		out = append(out, m)
	}
	sort.Strings(out)
	return out
}

// vnodeKey derives the hash input for node's id-th virtual node.
func vnodeKey(node string, id int) []byte {
	return []byte(node + "#" + strconv.Itoa(id))
}

// insertVNode adds v to the ring, keeping positions sorted. Callers must
// hold the write lock.
func (r *Ring) insertVNode(v vnode) {
	bucket, ok := r.buckets[v.pos]
	if !ok {
		i := sort.Search(len(r.positions), func(i int) bool { return r.positions[i] >= v.pos })
		r.positions = append(r.positions, 0)
		copy(r.positions[i+1:], r.positions[i:])
		r.positions[i] = v.pos
	}
	bucket = append(bucket, v)
	sort.Slice(bucket, func(a, b int) bool {
		if bucket[a].node != bucket[b].node {
			return bucket[a].node < bucket[b].node
		}
		return bucket[a].id < bucket[b].id
	})
	r.buckets[v.pos] = bucket
}

// removeVNode deletes the exact vnode (node, id) at position pos, leaving
// any colliding vnodes of other nodes in place. Callers must hold the
// write lock.
func (r *Ring) removeVNode(v vnode) {
	bucket, ok := r.buckets[v.pos]
	if !ok {
		return
	}
	for i, e := range bucket {
		if e.node == v.node && e.id == v.id {
			bucket = append(bucket[:i], bucket[i+1:]...)
			break
		}
	}
	if len(bucket) == 0 {
		delete(r.buckets, v.pos)
		i := sort.Search(len(r.positions), func(i int) bool { return r.positions[i] >= v.pos })
		if i < len(r.positions) && r.positions[i] == v.pos {
			r.positions = append(r.positions[:i], r.positions[i+1:]...)
		}
		return
	}
	r.buckets[v.pos] = bucket
}
