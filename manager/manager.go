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
	"github.com/att-innovate/charmander-scheduler/mesosproto"
)

type Manager interface {
	Start() error

	GetListenerIP() string
	GetListenerPortForScheduler() int
	GetListenerPortForRESTApi() int

	GetRedisConnectionIPAndPort() string

	GetTasks() []*Task
	GetNodes() []*Node

	GetOpenTaskRequestsNoSla() []*Task
	ResourceRequirementsWouldMatch(offer *mesosproto.Offer, taskRequest *Task) bool

	GetOpenTaskRequests() []*Task
	GetRunningTasks() []*Task

	SetTaskIntelligence(taskname string, attribute string, value string)
	GetTaskIntelligence(taskname string, attribute string) string

	HandleFrameworkRegistered(frameworkId string)
	HandleResourceOffered(offers []*mesosproto.Offer)
	HandleStatusMessage(statusMessage *mesosproto.StatusUpdateMessage)
	HandleRunDockerImage(task *Task)
	HandleDeleteTask(task *Task)
	HandleReshuffleTasks()

	AcceptOffer(offerId *mesosproto.OfferID, slaveId *mesosproto.SlaveID, task *Task)
	DeclineOffer(offerId *mesosproto.OfferID)
}
