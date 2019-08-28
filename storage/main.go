package storage

import (
	"strings"

	"lsmgo/substr/BoyerMooreHorspool"
)

type storage struct {
	tree *Node
}

type Node struct {
	key       string
	value     string
	height    int8
	leftNode  *Node
	rightNode *Node
}

func height(node *Node) int8 {
	if node != nil {
		return node.height
	}
	return 0
}

func bfactor(node *Node) int8 {
	return height(node.rightNode) - height(node.leftNode)
}

func fixHeight(node *Node) {
	leftNodeHeight := height(node.leftNode)
	rightNodeHeight := height(node.rightNode)

	if leftNodeHeight > rightNodeHeight {
		node.height = leftNodeHeight + 1
	} else {
		node.height = rightNodeHeight + 1
	}
}

func rotateRight(node *Node) *Node {
	leftNode := node.leftNode
	node.leftNode = leftNode.rightNode
	leftNode.rightNode = node
	fixHeight(node)
	fixHeight(leftNode)
	return leftNode
}

func rotateLeft(node *Node) *Node {
	rightNode := node.rightNode
	node.rightNode = rightNode.leftNode
	rightNode.leftNode = node
	fixHeight(node)
	fixHeight(rightNode)
	return rightNode
}

func balance(node *Node) *Node {
	fixHeight(node)

	if bfactor(node) == 2 {
		if bfactor(node.rightNode) < 0 {
			node.rightNode = rotateRight(node.rightNode)
		}
		node = rotateLeft(node)
	}
	if bfactor(node) == -2 {
		if bfactor(node.leftNode) > 0 {
			node.leftNode = rotateLeft(node.leftNode)
		}
		node = rotateRight(node)
	}
	return node
}

func insert(node *Node, key string, value string) *Node {
	if node == nil {
		return &Node{key: key, height: 1, value: value}
	}

	if node.key == key {
		node.value = value
	} else if key < node.key {
		node.leftNode = insert(node.leftNode, key, value)
	} else {
		node.rightNode = insert(node.rightNode, key, value)
	}
	return balance(node)
}

func initTree(key string, value string) *Node {
	node := &Node{key: key, height: 1, value: value}
	return node
}

func findByKey(node *Node, key string) string {
	if node == nil {
		return "No such element"
	}
	if node.key == key {
		if node.value == "/_lsmgo_deleted/" {
			return "No such element"
		}
		return node.value
	} else if node.key > key {
		return findByKey(node.leftNode, key)
	} else {
		return findByKey(node.rightNode, key)
	}
}

//if need check root before knot
func preOrderFindByValue(node *Node, desiredValue string, isSubstring bool, response *strings.Builder) {
	if node == nil {
		return
	}

	lenstr := len(desiredValue)
	if (node.value == desiredValue) || (isSubstring && (lenstr < 20 && strings.Contains(node.value, desiredValue)) || lenstr >= 20 && BoyerMooreHorspool.Contains(node.value, desiredValue)) {
		response.WriteString(node.key)
		response.WriteString(":")
		response.WriteString(node.value)
		response.WriteString("\n")
	}
	preOrderFindByValue(node.leftNode, desiredValue, isSubstring, response)
	preOrderFindByValue(node.rightNode, desiredValue, isSubstring, response)
}

func findByValue(node *Node, value string, isSubstring bool) string {
	var response strings.Builder
	preOrderFindByValue(node, value, isSubstring, &response)
	return response.String()
}

func updateValue(node *Node, key string, value string) string {
	if node == nil {
		return "Can't update. No such element"
	}
	if node.key == key {
		if node.value == "/_lsmgo_deleted/" {
			return "Can't update. No such element"
		}
		node.value = value
		return "Updated!"
	} else if node.key > key {
		return updateValue(node.leftNode, key, value)
	} else {
		return updateValue(node.rightNode, key, value)
	}
}

type storable interface {
	FindByKey(key string) string
	FindByValue(value string, isSubstring bool) string
	Insert(key string, value string)
	Update(key string, value string) string
}

func (storage *storage) FindByKey(key string) string {
	if storage.tree == nil {
		return "Storage is empty"
	} else {
		return findByKey(storage.tree, key)
	}
}

func (storage *storage) FindByValue(value string, isSubstring bool) string {
	if storage.tree == nil {
		return "Storage is empty"
	} else {
		return findByValue(storage.tree, value, isSubstring)
	}
}

func (storage *storage) Insert(key string, value string) {
	if storage.tree == nil {
		storage.tree = initTree(key, value)
	} else {
		storage.tree = insert(storage.tree, key, value)
	}
}

func (storage *storage) Update(key string, value string) string {
	if storage.tree == nil {
		return "Storage is empty"
	} else {
		return updateValue(storage.tree, key, value)
	}
}

var Storage storable = initStorage()

func initStorage() *storage {
	return &storage{}
}
