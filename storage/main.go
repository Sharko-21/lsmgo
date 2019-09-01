package storage

import (
	"io"
	"os"
	"strconv"
	"strings"

	"lsmgo/substr/BoyerMooreHorspool"
)

var sstRecordAdditionalSize = 6
var limitTreeSize = 8000000 /* ~7.63mb */
var indexBlockSize = os.Getpagesize()

type storage struct {
	tree *Node
	size int
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

func insert(storage *storage, node *Node, key string, value string) *Node {
	if node == nil {
		storage.size += len([]byte(value+key)) + sstRecordAdditionalSize
		return &Node{key: key, height: 1, value: value}
	}

	if node.key == key {
		storage.size += len([]byte(value)) - len([]byte(node.value))
		node.value = value
	} else if key < node.key {
		node.leftNode = insert(storage, node.leftNode, key, value)
	} else {
		node.rightNode = insert(storage, node.rightNode, key, value)
	}
	return balance(node)
}

func initTree(storage *storage, key string, value string) *Node {
	node := &Node{key: key, height: 1, value: value}
	storage.size += len([]byte(key + value))
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

func ssTableWrite(key string, value string, writeSessionData *writeSession, response io.Writer, index io.Writer) {
	writeData := []byte("key:" + key + ";value:" + value + ";")
	response.Write(writeData)
	writeSessionData.wroteIndexBytesLen += len(writeData)

	if writeSessionData.wroteIndexBytesLen-len(writeData) == 0 {
		indexWriteData := []byte(key + ":0;")
		writeSessionData.lastWroteIndex = 0
		index.Write(indexWriteData)
	} else if writeSessionData.wroteIndexBytesLen-writeSessionData.lastWroteIndex > indexBlockSize {
		indexOffset := writeSessionData.wroteIndexBytesLen - len(writeData)
		indexWriteData := []byte(key + ":" + strconv.FormatInt(int64(indexOffset), 10) + ";")
		writeSessionData.lastWroteIndex = indexOffset
		index.Write(indexWriteData)
	}
}

func preOrderSSTableWrite(node *Node, writeSessionData *writeSession, response io.Writer, index io.Writer) {
	if node == nil {
		return
	}

	preOrderSSTableWrite(node.leftNode, writeSessionData, response, index)
	ssTableWrite(node.key, node.value, writeSessionData, response, index)
	preOrderSSTableWrite(node.rightNode, writeSessionData, response, index)
}

//if need check root before knot
func preOrderFindByValue(node *Node, desiredValue string, isSubstring bool, response io.Writer) {
	if node == nil {
		return
	}

	lenstr := len(desiredValue)
	if (node.value == desiredValue) || (isSubstring && (lenstr < 20 && strings.Contains(node.value, desiredValue)) || lenstr >= 20 && BoyerMooreHorspool.Contains(node.value, desiredValue)) {
		response.Write([]byte(node.key))
		response.Write([]byte(":"))
		response.Write([]byte(node.value))
		response.Write([]byte("\n"))
	}
	preOrderFindByValue(node.leftNode, desiredValue, isSubstring, response)
	preOrderFindByValue(node.rightNode, desiredValue, isSubstring, response)
}

func findByValue(node *Node, value string, isSubstring bool) string {
	var response strings.Builder
	preOrderFindByValue(node, value, isSubstring, &response)
	if response.String() == "" {

	}
	return response.String()
}

func updateValue(storage *storage, node *Node, key string, value string) string {
	if node == nil {
		return "Can't update. No such element"
	}
	if node.key == key {
		if node.value == "/_lsmgo_deleted/" {
			return "Can't update. No such element"
		}
		storage.size += len([]byte(value)) - len([]byte(node.value))
		node.value = value
		return "Updated!"
	} else if node.key > key {
		return updateValue(storage, node.leftNode, key, value)
	} else {
		return updateValue(storage, node.rightNode, key, value)
	}
}

type storable interface {
	FindByKey(key string) string
	FindByValue(value string, isSubstring bool) string
	Insert(key string, value string)
	Update(key string, value string) string
	Shutdown()
}

func (storage *storage) FindByKey(key string) string {
	if storage.tree == nil {
		return ReadFromSSTables(key)
	} else {
		value := findByKey(storage.tree, key)
		if value == "No such element" {
			value = ReadFromSSTables(key)
		}
		return value
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
		storage.tree = initTree(storage, key, value)
	} else {
		storage.tree = insert(storage, storage.tree, key, value)
	}

	if storage.size > limitTreeSize {
		writeToSstable(storage)
	}
}

func (storage *storage) Update(key string, value string) string {
	if storage.tree == nil {
		return "Storage is empty"
	} else {
		return updateValue(storage, storage.tree, key, value)
	}
}

func (storage *storage) Shutdown() {
	if storage.tree != nil {
		writeToSstable(storage)
	}
}

var Storage storable = initStorage()

func initStorage() *storage {
	return &storage{}
}
