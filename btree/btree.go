package btree

import (
	"bytes"
	"encoding/binary"
)

const HEADER = 4

const BTREE_PAGE_SIZE = 4096
const BTREE_MAX_KEY_SIZE = 1000
const BTREE_MAX_VAL_SIZE = 3000

type BTree struct {
	// root pointer (a nonzero page number)
	root uint64

	// callbacks for managing on-disk pages
	get func(uint64) []byte // read data from a page number
	new func([]byte) uint64 // allocate a new page number with data
	del func(uint64)        // deallocate a page number
}

func treeInsert(tree *BTree, node BNode, key, val []byte) BNode {
	// The extra size allows it to exceed 1 page temporarily.
	newNode := BNode(make([]byte, 2*BTREE_PAGE_SIZE))

	// where to insert the key?
	idx := nodeLookupLE(node, key) // node.getKey(idx) <= key

	switch node.btype() {
	case BNODE_LEAF:
		if bytes.Equal(key, node.getKey(idx)) {
			leafUpdate(newNode, node, idx, key, val) // found, update it
		} else {
			leafInsert(newNode, node, idx+1, key, val) // not found, insert
		}
	case BNODE_NODE:
		// recusive insertion to the kid node
		kptr := node.getPtr(idx)
		knode := treeInsert(tree, tree.get(kptr), key, val)

		// after insertion, split the result
		nsplit, split := nodeSplit3(knode)

		// deallocate the old kid node
		tree.del(kptr)

		// update the kid links
		nodeReplaceKidN(tree, newNode, node, idx, split[:nsplit]...)
	}

	return newNode
}

/*
# Node:

	| type | nkeys |  pointers  |   offsets  | key-values | unused |
	|  2B  |   2B  | nkeys * 8B | nkeys * 2B |     ...    |        |

# Key-Value:

	| klen | vlen | key | val |
	|  2B  |  2B  | ... | ... |
*/
type BNode []byte // can be dumped to the disk

const (
	BNODE_NODE = 1 // internal nodes with pointers
	BNODE_LEAF = 2 // leaf nodes with values
)

// getters

// Returns the type of node this is, it can either be a `BNODE_NODE` or a `BNODE_LEAF`.
func (node BNode) btype() uint16 {
	return binary.LittleEndian.Uint16(node[0:2])
}

// Returns the amount of keys this node has or the number of childs it has.
func (node BNode) nkeys() uint16 {
	return binary.LittleEndian.Uint16(node[2:4])
}

// Sets the the node type and amount of keys for this node.
func (node BNode) setHeader(btype, nkeys uint16) {
	binary.LittleEndian.PutUint16(node[0:2], btype)
	binary.LittleEndian.PutUint16(node[2:4], nkeys)
}

// Read the nth child pointer.
func (node BNode) getPtr(idx uint16) uint64 {
	pos := HEADER + 8*idx
	return binary.LittleEndian.Uint64(node[pos:])
}

// Write the nth child pointer.
func (node BNode) setPtr(idx uint16, val uint64) {
	pos := HEADER + 8*idx
	binary.LittleEndian.PutUint64(node[pos:], val)
}

// offset list

// Returns the position for the nth offset.
func offsetPos(node BNode, idx uint16) uint16 {
	return HEADER + 8*node.nkeys() + 2*(idx-1)
}

// Read the `offsets` array.
func (node BNode) getOffset(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}

	pos := offsetPos(node, idx)
	return binary.LittleEndian.Uint16(node[pos:])
}

// Write the `offsets` array.
func (node BNode) setOffset(idx, val uint16) {
	if idx == 0 {
		return
	}

	pos := offsetPos(node, idx)
	binary.LittleEndian.PutUint16(node[pos:], val)
}

// key-values

// Returns the position of the nth KV pair relative to the whole node.
func (node BNode) kvPos(idx uint16) uint16 {
	keys := node.nkeys()
	return HEADER + 8*keys + 2*keys + node.getOffset(idx)
}

// Returns the nth key.
func (node BNode) getKey(idx uint16) []byte {
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node[pos:])
	return node[pos+4:][:klen]
}

func (node BNode) getVal(idx uint16) []byte {
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node[pos:])
	vlen := binary.LittleEndian.Uint16(node[pos+2:])
	return node[pos+4+klen:][:vlen]
}

// node size in bytes
func (node BNode) nbytes() uint16 {
	return node.kvPos(node.nkeys())
}

// find the last position that is less than or equal to the key.
func nodeLookupLE(node BNode, key []byte) uint16 {
	nkeys := node.nkeys()
	var i uint16

	for i = uint16(1); i < nkeys; i++ {
		cmp := bytes.Compare(node.getKey(i), key)
		if cmp <= 0 {
			return i
		}
		if cmp >= 0 {
			return i - 1
		}
	}

	return i - 1
}

func nodeAppendKV(node BNode, idx uint16, ptr uint64, key, val []byte) {
	// ptrs
	node.setPtr(idx, ptr)

	// KVs
	pos := node.kvPos(idx)

	// 4-bytes KV sizes
	binary.LittleEndian.PutUint16(node[pos:], uint16(len(key)))
	binary.LittleEndian.PutUint16(node[pos+2:], uint16(len(val)))

	// KV data
	copy(node[pos+4:], key)
	copy(node[pos+4+uint16(len(key)):], val)

	// update the offset value for the next key
	node.setOffset(idx+1, node.getOffset(idx)+4+uint16((len(key)+len(val))))
}

func nodeAppendRange(newNode, oldNode BNode, dstNew, srcOld, n uint16) {
	for i := uint16(0); i < n; i++ {
		dst, src := dstNew+i, srcOld+i
		nodeAppendKV(newNode, dst, oldNode.getPtr(src), oldNode.getKey(src), oldNode.getVal(src))
	}
}

func leafInsert(newNode, oldNode BNode, idx uint16, key, val []byte) {
	newNode.setHeader(BNODE_LEAF, oldNode.nkeys()+1)
	nodeAppendRange(newNode, oldNode, 0, 0, idx)                       // copy the keys before `idx`
	nodeAppendKV(newNode, idx, 0, key, val)                            // the new key
	nodeAppendRange(newNode, oldNode, idx+1, idx, oldNode.nkeys()-idx) // keys from `idx`
}

func leafUpdate(newNode, oldNode BNode, idx uint16, key, val []byte) {
	newNode.setHeader(BNODE_LEAF, oldNode.nkeys())
	nodeAppendRange(newNode, oldNode, 0, 0, idx)
	nodeAppendKV(newNode, idx, 0, key, val)
	nodeAppendRange(newNode, oldNode, idx+1, idx+1, oldNode.nkeys()-(idx+1))
}

// Split an oversized node into 2 nodes. The 2nd node always fits.
func nodeSplit2(left, right, old BNode) {
	// the initial guess
	nleft := old.nkeys() / 2

	// try to fit the left half
	left_bytes := func() uint16 {
		return HEADER + 8*nleft + 2*nleft + old.getOffset(nleft)
	}
	for left_bytes() > BTREE_PAGE_SIZE {
		nleft--
	}

	// try to fit the right half
	right_bytes := func() uint16 {
		return old.nbytes() - left_bytes() + HEADER
	}
	for right_bytes() > BTREE_PAGE_SIZE {
		nleft++
	}

	nright := old.nkeys() - nleft

	// new nodes
	left.setHeader(old.btype(), nleft)
	right.setHeader(old.btype(), nright)
	nodeAppendRange(left, old, 0, 0, nleft)
	nodeAppendRange(right, old, 0, nleft, nright)
}

// replace a link with multiple links
func nodeReplaceKidN(tree *BTree, newNode, oldNode BNode, idx uint16, kids ...BNode) {
	inc := uint16(len(kids))
	newNode.setHeader(BNODE_NODE, oldNode.nkeys()+inc-1)
	nodeAppendRange(newNode, oldNode, 0, 0, idx)

	for i, node := range kids {
		nodeAppendKV(newNode, idx+uint16(i), tree.new(node), node.getKey(0), nil)
	}

	nodeAppendRange(newNode, oldNode, idx+inc, idx+1, oldNode.nkeys()-(idx+1))
}

// Split a node if it's too big. The results are 1~3 nodes.
func nodeSplit3(old BNode) (uint16, [3]BNode) {
	if old.nbytes() <= BTREE_PAGE_SIZE {
		old = old[:BTREE_PAGE_SIZE]
		return 1, [3]BNode{old} // not split
	}

	left := BNode(make([]byte, 2*BTREE_PAGE_SIZE)) // might be split later
	right := BNode(make([]byte, BTREE_PAGE_SIZE))
	nodeSplit2(left, right, old)

	if left.nbytes() <= BTREE_PAGE_SIZE {
		left = left[:BTREE_PAGE_SIZE]
		return 2, [3]BNode{left, right} // 2 nodes
	}

	leftleft := BNode(make([]byte, BTREE_PAGE_SIZE))
	middle := BNode(make([]byte, BTREE_PAGE_SIZE))
	nodeSplit2(leftleft, middle, left)
	return 3, [3]BNode{leftleft, middle, right} // 3 nodes
}
