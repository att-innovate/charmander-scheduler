// The MIT License (MIT)
//
// Copyright (c) 2014 AT&T
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package impl

import (
	"errors"
	"sync"
	"time"

	managerInterface "github.com/att-innovate/charmander-scheduler/manager"
)

var ErrNodeNotExists = errors.New("node does not exist")

type NodeRegistry struct {
	nodes map[string]*managerInterface.Node
	sync.RWMutex
}

func NewNodeRegistry() *NodeRegistry {
	return &NodeRegistry{
		nodes: make(map[string]*managerInterface.Node),
	}
}

func (nodeRegistry *NodeRegistry) Register(id string, node *managerInterface.Node) {
	nodeRegistry.Lock()
	defer nodeRegistry.Unlock()

	nodeRegistry.nodes[id] = node
}

func (nodeRegistry *NodeRegistry) Exists(id string) bool {
	_, exists := nodeRegistry.nodes[id]
	return exists
}

func (nodeRegistry *NodeRegistry) Fetch(id string) (*managerInterface.Node, error) {
	nodeRegistry.RLock()
	defer nodeRegistry.RUnlock()

	node, exists := nodeRegistry.nodes[id]
	if !exists {
		return nil, ErrNodeNotExists
	}

	return node, nil
}

func (nodeRegistry *NodeRegistry) UpdateTimeOfLastOffer(id string) {
	nodeRegistry.Lock()
	defer nodeRegistry.Unlock()

	nodeRegistry.nodes[id].TimeOfLastOffer = time.Now().Unix()
}

func (nodeRegistry *NodeRegistry) Update(id string, node *managerInterface.Node) {
	nodeRegistry.Lock()
	defer nodeRegistry.Unlock()

	nodeRegistry.nodes[id] = node
}

func (nodeRegistry *NodeRegistry) Delete(id string) {
	nodeRegistry.Lock()
	defer nodeRegistry.Unlock()

	delete(nodeRegistry.nodes, id)
}

func (nodeRegistry *NodeRegistry) Nodes() ([]*managerInterface.Node) {
	nodeRegistry.RLock()
	defer nodeRegistry.RUnlock()

	i :=  0
	result := make([]*managerInterface.Node, len(nodeRegistry.nodes))

	for _, value := range nodeRegistry.nodes {
		result[i] = value
		i++
	}

	return result
}

