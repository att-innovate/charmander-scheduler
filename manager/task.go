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

package manager

import (
	"encoding/json"

	"github.com/golang/glog"
	"github.com/att-innovate/charmander-scheduler/mesosproto"
)

const (
	SLA_ONE_PER_NODE = "one-per-node"
	SLA_SINGLETON = "singleton"

	NETWORK_MODE_BRIDGE = "bridge"
	NETWORK_MODE_HOST = "host"
	NETWORK_MODE_NONE = "none"
)

type Task struct {
	ID            string    `json:"id"`
	DockerImage   string    `json:"dockerimage"`
	Mem           uint64    `json:"mem,string"`
	Cpus          float64   `json:"cpus,string"`
	Sla           string    `json:"sla"`
	NodeType      string    `json:"nodetype"`
	NodeName      string    `json:"nodename"`
	NotMetered    bool      `json:"notmetered"`
	Reshuffleable bool      `json:"reshuffleable"`
	Arguments     []string  `json:"arguments,omitempty"`
	Volumes       []*Volume `json:"volumes,omitempty"`
	Ports         []*Port   `json:"ports,omitempty"`
	NetworkMode   string    `json:"networkmode"`

	InternalID    string
	SlaveID       string
	DockerID      string
	DockerName    string
	ProcessID     uint
	CreatedAt     int64
	TaskInfo     *mesosproto.TaskInfo
	RequestSent   bool
	Running       bool
}

func CopyTask(source Task) Task {
	result := &Task{}

	jsonEncoded, _ := json.Marshal(&source)
	json.Unmarshal(jsonEncoded, &result)

	return *result
}

func ResetTask(task *Task) {
	task.InternalID = ""
	task.SlaveID = ""
	task.DockerID = ""
	task.DockerName = ""
	task.ProcessID = 0
	task.CreatedAt = 0
	task.TaskInfo = nil
	task.RequestSent = false
	task.Running = false
}

// Handling Docker Inspect Output

type DockerTask struct {
	DockerId string `json:"Id"`
	DockerName string `json:"Name"`
	DockerState json.RawMessage `json:"State"`
}

type DockerState struct {
	Pid uint `json:"Pid"`
}

func UpdateTaskWithDockerInfo(task *Task, dockerInspectOutput []byte) {
//	glog.Infof("Docker Inspect: %s", dockerInspectOutput)
	var dockerTasks []DockerTask
	err := json.Unmarshal(dockerInspectOutput, &dockerTasks)
	if err != nil {
		glog.Errorf("UpdateTaskWithDockerInfo error: %v\n", err)
		return
	}
	task.DockerID = dockerTasks[0].DockerId
	task.DockerName = dockerTasks[0].DockerName[1:]

	var dockerState DockerState
	err  = json.Unmarshal(dockerTasks[0].DockerState, &dockerState)
	if err != nil {
		glog.Errorf("UpdateTaskWithDockerInfo error: %v\n", err)
		return
	}

	task.ProcessID = dockerState.Pid
}