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

	managerInterface "github.com/att-innovate/charmander-scheduler/manager"
)

var ErrTaskNotExists = errors.New("task does not exist")

type TaskRegistry struct {
	tasks map[string]*managerInterface.Task
	sync.RWMutex
}

func NewTaskRegistry() *TaskRegistry {
	return &TaskRegistry{
		tasks: make(map[string]*managerInterface.Task),
	}
}

func (taskRegistry *TaskRegistry) Register(id string, task *managerInterface.Task) {
	taskRegistry.Lock()
	defer taskRegistry.Unlock()

	taskRegistry.tasks[id] = task
}

func (taskRegistry *TaskRegistry) Fetch(id string) (*managerInterface.Task, error) {
	taskRegistry.RLock()
	defer taskRegistry.RUnlock()

	task, exists := taskRegistry.tasks[id]
	if !exists {
		return nil, ErrTaskNotExists
	}

	return task, nil
}

func (taskRegistry *TaskRegistry) TimeOfLastOffer(id string, task *managerInterface.Task) {
	taskRegistry.Lock()
	defer taskRegistry.Unlock()

	taskRegistry.tasks[id] = task
}

func (taskRegistry *TaskRegistry) Delete(id string) {
	taskRegistry.Lock()
	defer taskRegistry.Unlock()

	delete(taskRegistry.tasks, id)
}

func (taskRegistry *TaskRegistry) Tasks() ([]*managerInterface.Task) {
	taskRegistry.RLock()
	defer taskRegistry.RUnlock()

	i :=  0
	result := make([]*managerInterface.Task, len(taskRegistry.tasks))

	for _, task := range taskRegistry.tasks {
		result[i] = task
		i++
	}

	return result
}

type filter func(task *managerInterface.Task) bool

func (taskRegistry *TaskRegistry) OpenTaskRequests() ([]*managerInterface.Task) {
	return taskRegistry.filterTasks(func(task *managerInterface.Task) bool {return task.RequestSent})
}

func (taskRegistry *TaskRegistry) OpenTaskRequestsNoSla() ([]*managerInterface.Task) {
	return taskRegistry.filterTasks(func(task *managerInterface.Task) bool {return task.RequestSent || len(task.Sla) > 0})
}

func (taskRegistry *TaskRegistry) RunningTasks() ([]*managerInterface.Task) {
	return taskRegistry.filterTasks(func(task *managerInterface.Task) bool {return task.Running == false})
}

func (taskRegistry *TaskRegistry) filterTasks(suppress filter) ([]*managerInterface.Task) {
	taskRegistry.RLock()
	defer taskRegistry.RUnlock()

	result := make([]*managerInterface.Task, 0)

	for _, task := range taskRegistry.tasks {
		if suppress(task) { continue }
		result = append(result, task)
	}

	return result
}
