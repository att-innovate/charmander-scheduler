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
	"os"
	"fmt"
	"time"
	"strings"

	"code.google.com/p/gogoprotobuf/proto"
	"github.com/golang/glog"

	"github.com/att-innovate/charmander-scheduler/communication"
	"github.com/att-innovate/charmander-scheduler/mesosproto"
	"github.com/att-innovate/charmander-scheduler/scheduler"
	"github.com/att-innovate/charmander-scheduler/upid"

	managerInterface "github.com/att-innovate/charmander-scheduler/manager"
)


var taskRegistry = NewTaskRegistry()
var nodeRegistry = NewNodeRegistry()

func New(
	scheduler *scheduler.Scheduler,
	frameworkName string,
	master string,
	listening string) (manager, error) {

	framework := &mesosproto.FrameworkInfo{
		User: proto.String(""),
		Name: proto.String(frameworkName),
	}

	newManager := manager{
		scheduler:     scheduler,
		frameworkInfo: framework,
		master:        master,
		listening:     listening,
	}

	return newManager, nil
}

type manager struct {
	scheduler     *scheduler.Scheduler
	frameworkInfo *mesosproto.FrameworkInfo
	masterUPID    *upid.UPID
	selfUPID      *upid.UPID
	frameworkId   string
	master        string
	listening     string
}

func (self *manager) Start() error {
	if communication.MesosMasterReachable(self.master) == false {
		return errors.New("Mesos unreachable")
	}

	self.frameworkInfo.User = proto.String("root")

	// set default hostname
	if self.frameworkInfo.GetHostname() == "" {
		host, err := os.Hostname()
		if err != nil || host == "" {
			host = "unknown"
		}
		self.frameworkInfo.Hostname = proto.String(host)
	}

	if m, err := upid.Parse("master@" + self.master); err != nil {
		return err
	} else {
		self.masterUPID = m
	}

	self.selfUPID= &upid.UPID{
		ID: "scheduler",
		Host: self.GetListenerIP(),
		Port: fmt.Sprintf("%d", self.GetListenerPortForScheduler())}

	communication.InitRestHandler(self)

	self.announceFramework()

	return nil
}

func (self *manager) GetListenerIP() string {
	return self.listening
}

func (self *manager) GetListenerPortForScheduler() int {
	return 7070
}

func (self *manager) GetListenerPortForRESTApi() int {
	return 7075
}

func (self *manager) GetTaskRequests() []*managerInterface.Task {
	return taskRegistry.Tasks()
}

func (self *manager) HandleFrameworkRegistered(frameworkId string) {
	self.frameworkId = frameworkId
	if self.scheduler != nil && self.scheduler.Registered != nil {
		self.scheduler.Registered(self, frameworkId)
	}
}

func (self *manager) HandleResourceOffered(offers []*mesosproto.Offer) {
	self.updateNodeRegistry(offers)

	offers= self.enforceSLAs(offers)

	if self.scheduler != nil && self.scheduler.ResourceOffers != nil {
		self.scheduler.ResourceOffers(self, offers)
	}
}

func (self *manager) updateNodeRegistry(offers []*mesosproto.Offer) {
	for _, offer := range offers {
		slaveID := offer.GetSlaveId().GetValue()
		if nodeRegistry.Exists(slaveID) {
			nodeRegistry.UpdateTimeOfLastOffer(slaveID)
			continue
		}

		node := &managerInterface.Node {
			ID: slaveID,
			Hostname: offer.GetHostname(),
			TimeOfLastOffer: time.Now().Unix(),
		}

		for _, attribute := range offer.GetAttributes() {
			if *attribute.Name == "nodename" {
				node.NodeName = attribute.Text.GetValue()
			} else if *attribute.Name == "nodetype" {
				node.NodeType = attribute.Text.GetValue()
			}
		}

		nodeRegistry.Register(slaveID, node)

		glog.Infoln("New Node Registered: ", node)
	}
}

func (self *manager) enforceSLAs(offers []*mesosproto.Offer) []*mesosproto.Offer {
	var taskRequests []*managerInterface.Task
	taskRequests = self.GetTaskRequests()

	for _, taskRequest := range taskRequests {
		if taskRequest.Running { continue }
		if len(taskRequest.Sla) == 0 {continue }

		if taskRequest.Sla == managerInterface.SLA_ONE_PER_NODE {
			for _, offer := range offers {
				if resolveNodeName(*offer) == taskRequest.NodeName {
					self.AcceptOffer(offer.GetId(), offer.SlaveId, taskRequest)
				}
			}
		}
	}

	return offers
}

func resolveNodeName(offer mesosproto.Offer) string {
	for _, attribute := range offer.GetAttributes() {
		if *attribute.Name == "nodename" {
			return attribute.Text.GetValue()
		}
	}

	return "notfound"
}

func (self *manager) HandleStatusMessage(statusMessage *mesosproto.StatusUpdateMessage) {
	glog.Infof("Status Update %v\n", statusMessage)
	status := statusMessage.GetUpdate().GetStatus()

	switch {
	case  *status.State == mesosproto.TaskState_TASK_RUNNING:
		task, _ := taskRegistry.Fetch(status.GetTaskId().GetValue())
		task.Running= true
		glog.Infoln("Running: ", task.InternalID)
	case  *status.State == mesosproto.TaskState_TASK_FAILED:
		taskRegistry.Delete(status.GetTaskId().GetValue())
		glog.Infoln("Task Failed: ", status.GetTaskId().GetValue())
	case  *status.State == mesosproto.TaskState_TASK_LOST:
		task, _ := taskRegistry.Fetch(status.GetTaskId().GetValue())
		task.RequestSent= false
		glog.Infoln("Lost: ", task.InternalID)
	case  *status.State == mesosproto.TaskState_TASK_FINISHED:
		taskRegistry.Delete(status.GetTaskId().GetValue())
		glog.Infoln("Task Finished: ", status.GetTaskId().GetValue())
	}

	self.acknowledgeStatusUpdate(statusMessage)
}

func (self *manager) HandleRunDockerImage(task *managerInterface.Task) {
	if task.Sla == managerInterface.SLA_ONE_PER_NODE {
		for _, node := range nodeRegistry.Nodes() {
			newTask := managerInterface.CopyTask(*task)
			newTask.NodeName= node.NodeName
			self.handleRunDockerImageImpl(&newTask)
		}
	} else {
		self.handleRunDockerImageImpl(task)
	}
}

func (self *manager) handleRunDockerImageImpl(task *managerInterface.Task) {
	id := fmt.Sprintf("%v-%v", strings.Replace(task.ID, " ", "", -1), time.Now().UnixNano())
	memory := float64(task.Mem)
	arguments := strings.Split(task.Arguments, " ")
	portResources := []*mesosproto.Value_Range{}
	cpus := float64(0.1)
	if task.Cpus > 0 {
		cpus= task.Cpus
	}

	dockerInfo := &mesosproto.ContainerInfo_DockerInfo {
		Image: &task.DockerImage,
	}
	containerInfo := &mesosproto.ContainerInfo {
		Type: mesosproto.ContainerInfo_DOCKER.Enum(),
		Docker: dockerInfo,
	}
	for _, volume := range task.Volumes {
		mode := mesosproto.Volume_RW
		if volume.Mode == "ro" {
			mode = mesosproto.Volume_RO
		}

		containerInfo.Volumes = append(containerInfo.Volumes, &mesosproto.Volume{
				ContainerPath: &volume.ContainerPath,
				HostPath:      &volume.HostPath,
				Mode:          &mode,
			})
	}
	for _, port := range task.Ports {
		dockerInfo.PortMappings = append(dockerInfo.PortMappings, &mesosproto.ContainerInfo_DockerInfo_PortMapping{
				ContainerPort: &port.ContainerPort,
				HostPort:      &port.HostPort,
			})
		portResources = append(portResources, &mesosproto.Value_Range{
				Begin: proto.Uint64(uint64(port.HostPort)),
				End:   proto.Uint64(uint64(port.HostPort)),
			})
	}
	if len(task.Ports) > 0 {
		dockerInfo.Network= mesosproto.ContainerInfo_DockerInfo_BRIDGE.Enum()
	}

	commandInfo := &mesosproto.CommandInfo {
		Shell: proto.Bool(false),
	}
	if len(arguments) > 0 {
		commandInfo.Arguments = arguments
	}

	resources := [] *mesosproto.Resource {
		&mesosproto.Resource{
			Name: proto.String("mem"),
			Scalar: &mesosproto.Value_Scalar{Value: &memory},
			Type: mesosproto.Value_SCALAR.Enum(),
		},
		&mesosproto.Resource{
			Name: proto.String("cpus"),
			Scalar: &mesosproto.Value_Scalar{Value: &cpus},
			Type: mesosproto.Value_SCALAR.Enum(),
		},
	}
	if len(task.Ports) > 0 {
		resources = append(resources,
			&mesosproto.Resource{
				Name: proto.String("ports"),
				Ranges: &mesosproto.Value_Ranges{ Range: portResources},
				Type: mesosproto.Value_RANGES.Enum(),
			},
		)
	}

	taskInfo := &mesosproto.TaskInfo {
		Name: &id,
		TaskId: &mesosproto.TaskID{Value: &id},
		Container: containerInfo,
		Command: commandInfo,
		Resources: resources,
	}

	task.InternalID = id
	task.CreatedAt = time.Now().Unix()
	task.TaskInfo = taskInfo
	task.RequestSent = false

	glog.Infoln("Task: ", task)

	taskRegistry.Register(id, task)

}


func (self *manager) announceFramework() {
	message := &mesosproto.RegisterFrameworkMessage{
		Framework: self.frameworkInfo,
	}

	glog.Infof("Registering with master %s [%s] ", self.masterUPID, message)
	messagePackage := communication.NewMessage(self.masterUPID, message, nil)
	if err := communication.SendMessageToMesos(self.selfUPID, messagePackage); err != nil {
		glog.Errorf("Failed to send RegisterFramework message: %v\n", err)
	}

}

func (self *manager) AcceptOffer(offerId *mesosproto.OfferID, slaveId *mesosproto.SlaveID, taskRequest *managerInterface.Task) {
	glog.Infoln("Working on: ", taskRequest.TaskInfo)
	taskRequest.TaskInfo.SlaveId= slaveId
	message := &mesosproto.LaunchTasksMessage{
		FrameworkId: &mesosproto.FrameworkID{Value: &self.frameworkId},
		OfferIds:    []*mesosproto.OfferID{offerId},
		Tasks:       []*mesosproto.TaskInfo{taskRequest.TaskInfo},
		Filters:     &mesosproto.Filters{},
	}

	messagePackage := communication.NewMessage(self.masterUPID, message, nil)
	if err := communication.SendMessageToMesos(self.selfUPID, messagePackage); err != nil {
		glog.Errorf("Failed to send AcceptOffer message: %v\n", err)
	} else {
		taskRequest.RequestSent = true
	}

}

func (self *manager) DeclineOffer(offerId *mesosproto.OfferID) {
	message := &mesosproto.LaunchTasksMessage{
		FrameworkId: &mesosproto.FrameworkID{Value: &self.frameworkId},
		OfferIds:    []*mesosproto.OfferID{offerId},
		Tasks:       []*mesosproto.TaskInfo{},
		Filters:     &mesosproto.Filters{},
	}

	messagePackage := communication.NewMessage(self.masterUPID, message, nil)
	if err := communication.SendMessageToMesos(self.selfUPID, messagePackage); err != nil {
		glog.Errorf("Failed to send DeclineOffer message: %v\n", err)
	}

}

func (self *manager) acknowledgeStatusUpdate(statusUpdate *mesosproto.StatusUpdateMessage) {
	message := &mesosproto.StatusUpdateAcknowledgementMessage{
		FrameworkId: statusUpdate.GetUpdate().FrameworkId,
		SlaveId:     statusUpdate.GetUpdate().Status.SlaveId,
		TaskId:      statusUpdate.GetUpdate().Status.TaskId,
		Uuid:        statusUpdate.GetUpdate().Uuid,
	}

	messagePackage := communication.NewMessage(self.masterUPID, message, nil)
	if err := communication.SendMessageToMesos(self.selfUPID, messagePackage); err != nil {
		glog.Errorf("Failed to send StatusAccept message: %v\n", err)
	}

}
