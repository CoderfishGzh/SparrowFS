package master

import "sync"

type namespaceManager struct {
	root     *nsTree
	serialCt int
}

type nsTree struct {
	sync.RWMutex

	isDir    bool
	children map[string]*nsTree

	length int64
	chunks int64
}

type serialTreeNode struct {
	IsDir    bool
	Children map[string]int
	Chunks   int64
}

// tree2array , transforms the namespace tree into an array for serialization
// return the index of root in the serial list, and the list of serialized tree nodes
func (nm *namespaceManager) tree2array(array *[]serialTreeNode, node *nsTree) int {
	n := serialTreeNode{
		IsDir:  node.isDir,
		Chunks: node.chunks,
	}

	// this node is a directory
	if node.isDir {
		n.Children = make(map[string]int)
		for k, v := range node.children {
			// recursively call this function to get the children of this node
			n.Children[k] = nm.tree2array(array, v)
		}
	}

	// update the serialTreeNode list
	*array = append(*array, n)
	ret := nm.serialCt
	nm.serialCt++
	return ret
}

func (nm *namespaceManager) Serialize() []serialTreeNode {
	nm.root.RLock()
	defer nm.root.RUnlock()

	nm.serialCt = 0
	var ret []serialTreeNode
	nm.tree2array(&ret, nm.root)
	return ret
}

// array2tree transforms the an serialized array to namespace tree
// id is the root index of the serialTreeNode list
func (nm *namespaceManager) array2tree(array []serialTreeNode, id int) *nsTree {
	n := &nsTree{isDir: array[id].IsDir, chunks: array[id].Chunks}

	if array[id].IsDir {
		// crate the children map for this node
		n.children = make(map[string]*nsTree)
		// recursively call this function to get the children of this node
		// k is the name of the child, v is the index of the child in the serialTreeNode list
		for k, v := range array[id].Children {
			n.children[k] = nm.array2tree(array, v)
		}
	}

	return n
}

// Deserialize the metadata from disk
func (nm *namespaceManager) Deserialize(array []serialTreeNode) error {
	// lock the namespace
	nm.root.Lock()
	defer nm.root.Unlock()

	nm.root = nm.array2tree(array, len(array)-1)
	return nil
}