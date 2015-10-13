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

	"github.com/gogo/protobuf/proto"
	"github.com/golang/glog"

	"github.com/att-innovate/charmander-scheduler/communication"
	"github.com/att-innovate/charmander-scheduler/communication/redis"
	"github.com/att-innovate/charmander-scheduler/mesosproto"
	"github.com/att-innovate/charmander-scheduler/scheduler"
	"github.com/att-innovate/charmander-scheduler/upid"

	managerInterface "github.com/att-innovate/charmander-scheduler/manager"
)


var taskRegistry = NewTaskRegistry()
var nodeRegistry = NewNodeRegistry()

var taskIntelligence = make(map[string]string)

func New(
	scheduler *scheduler.Scheduler,
	frameworkName string,
	master string,
	listening string,
	redis string) (manager, error) {

	framework := &mesosproto.FrameworkInfo{
		User: proto.String(""),
		Name: proto.String(frameworkName),
	}

	newManager := manager{
		scheduler:     scheduler,
		frameworkInfo: framework,
		master:        master,
		listening:     listening,
		redis:         redis,
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
	redis         string
}

func (self *manager) Start() error {
	if self.verifyConnectionWithMesos() == false {
		return errors.New("Mesos unreachable")
	}

	self.frameworkInfo.User = proto.String("root")
	self.frameworkInfo.Checkpoint = proto.Bool(false)

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
	redis.InitRedisUpdater(self)

	//sleep some more to be ready to accept requests from Mesos
	time.Sleep(5 * time.Second)

	self.announceFramework()

	return nil
}

func (self *manager) verifyConnectionWithMesos() bool {
	retryCounter := 6

	for ; retryCounter > 0; retryCounter-- {
		if communication.MesosMasterReachable(self.master) { break }
		time.Sleep(10 * time.Second)
	}

	if retryCounter == 0 {
		return false
	} else {
		//just to be save that Mesos is ready to accept messages after a restart
		time.Sleep(5 * time.Second)
	}

	return true
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

func (self *manager) GetRedisConnectionIPAndPort() string {
	return self.redis
}

func (self *manager) GetTasks() []*managerInterface.Task {
	return taskRegistry.Tasks()
}

func (self *manager) GetNodes() []*managerInterface.Node {
	return nodeRegistry.Nodes()
}

func (self *manager) GetOpenTaskRequests() []*managerInterface.Task {
	return taskRegistry.OpenTaskRequests()
}

func (self *manager) GetOpenTaskRequestsNoSla() []*managerInterface.Task {
	return taskRegistry.OpenTaskRequestsNoSla()
}

func (self *manager) GetRunningTasks() []*managerInterface.Task {
	return taskRegistry.RunningTasks()
}

func (self *manager) SetTaskIntelligence(taskname string, attribute string, value string) {
	if len(value) > 0 {
		taskIntelligence[formatTaskIntelligenceKey(taskname, attribute)] = value
	} else {
		delete(taskIntelligence, formatTaskIntelligenceKey(taskname, attribute))
	}
}

func (self *manager) GetTaskIntelligence(taskname string, attribute string) string {
	return taskIntelligence[formatTaskIntelligenceKey(taskname, attribute)]
}

func formatTaskIntelligenceKey(taskname string, attribute string) string {
	return fmt.Sprintf("%s:%s", taskname, attribute)
}

func (self *manager) ResourceRequirementsWouldMatch(offer *mesosproto.Offer, taskRequest *managerInterface.Task) bool {
	for _, resource := range offer.GetResources() {
		switch {
		case *resource.Name == "cpus":
			if resource.Scalar.GetValue() < float64(taskRequest.Cpus) { return false }
		case *resource.Name == "mem":
			if resource.Scalar.GetValue() < float64(taskRequest.Mem) { return false }
		}
	}
	for _, attribute := range offer.GetAttributes() {
		switch {
		case *attribute.Name == "nodetype":
			if len(taskRequest.NodeType) > 0 && attribute.Text.GetValue() != taskRequest.NodeType { return false }
		case *attribute.Name == "nodename":
			if len(taskRequest.NodeName) > 0 && attribute.Text.GetValue() != taskRequest.NodeName { return false }
		}
	}

	return true
}

func (self *manager) HandleFrameworkRegistered(frameworkId string) {
	self.frameworkId = frameworkId
	if self.scheduler != nil && self.scheduler.Registered != nil {
		self.scheduler.Registered(self, frameworkId)
	}
}

func (self *manager) HandleResourceOffered(offers []*mesosproto.Offer) {
	self.updateNodeRegistry(offers)

	offers= self.handleSlaRequests(offers)

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

func (self *manager) handleSlaRequests(offers []*mesosproto.Offer) []*mesosproto.Offer {
	result := offers
	var taskRequests []*managerInterface.Task
	taskRequests = self.GetOpenTaskRequests()

	for _, taskRequest := range taskRequests {
		if taskRequest.Running { continue }
		if len(taskRequest.Sla) == 0 { continue }

		if taskRequest.Sla == managerInterface.SLA_ONE_PER_NODE || taskRequest.Sla == managerInterface.SLA_SINGLETON {
			for _, offer := range offers {
				if self.ResourceRequirementsWouldMatch(offer, taskRequest) {
					self.AcceptOffer(offer.GetId(), offer.SlaveId, taskRequest)
					result = removeOfferFromList(result, offer)
				}
			}
		}
	}

	return result
}

func removeOfferFromList(offers []*mesosproto.Offer, offer *mesosproto.Offer) []*mesosproto.Offer {
	numberOfOffers := len(offers)
	if numberOfOffers == 0 { return []*mesosproto.Offer {}}

	result := make([]*mesosproto.Offer, 0)
	for _, value := range offers {
		if offer.GetId().GetValue() == value.GetId().GetValue() { continue }
		result = append(result, value)
	}

	return result
}

func (self *manager) HandleStatusMessage(statusMessage *mesosproto.StatusUpdateMessage) {
	glog.Infof("Status Update %v\n", statusMessage)
	status := statusMessage.GetUpdate().GetStatus()

	switch {
	case  *status.State == mesosproto.TaskState_TASK_RUNNING:
		task, _ := taskRegistry.Fetch(status.GetTaskId().GetValue())
		task.Running= true
		task.SlaveID= status.GetSlaveId().GetValue()
		managerInterface.UpdateTaskWithDockerInfo(task, status.GetData())
	case  *status.State == mesosproto.TaskState_TASK_FAILED:
		taskRegistry.Delete(status.GetTaskId().GetValue())
		glog.Infoln("Task Failed: ", status.GetTaskId().GetValue())
	case  *status.State == mesosproto.TaskState_TASK_LOST:
		switch {
		case strings.Contains(status.GetMessage(), "Task has duplicate ID"):
		// ignore
		case strings.Contains(status.GetMessage(), "is no longer valid"):
			task, _ := taskRegistry.Fetch(status.GetTaskId().GetValue())
			task.RequestSent = false
		default:
			taskRegistry.Delete(status.GetTaskId().GetValue())
		}
		glog.Infoln("Task Lost: ", status.GetTaskId().GetValue())
	case  *status.State == mesosproto.TaskState_TASK_FINISHED:
		taskRegistry.Delete(status.GetTaskId().GetValue())
		glog.Infoln("Task Finished: ", status.GetTaskId().GetValue())
	case  *status.State == mesosproto.TaskState_TASK_KILLED:
		taskRegistry.Delete(status.GetTaskId().GetValue())
		glog.Infoln("Task Killed: ", status.GetTaskId().GetValue())
	}

	self.acknowledgeStatusUpdate(statusMessage)
}

func (self *manager) HandleRunDockerImage(task *managerInterface.Task) {
	if task.Sla == managerInterface.SLA_ONE_PER_NODE {
		if self.isDuplicateRunRequest(task) { return }
		for _, node := range nodeRegistry.Nodes() {
			newTask := managerInterface.CopyTask(*task)
			newTask.NodeName= node.NodeName
			self.handleRunDockerImageImpl(&newTask)
		}
	} else if task.Sla == managerInterface.SLA_SINGLETON {
		if self.isDuplicateRunRequest(task) { return }
		self.handleRunDockerImageImpl(task)
	} else {
		self.handleRunDockerImageImpl(task)
	}
}

func (self *manager) isDuplicateRunRequest(newTask *managerInterface.Task) bool {
	tasks := self.GetTasks()
	for _, task := range tasks {
		if task.ID == newTask.ID { return true }
	}
	return false
}

func (self *manager) handleRunDockerImageImpl(task *managerInterface.Task) {
	if self.scheduler != nil && self.scheduler.OverwriteTaskAttributes != nil {
		self.scheduler.OverwriteTaskAttributes(self, task)
	}

	id := fmt.Sprintf("%v-%v", strings.Replace(task.ID, " ", "", -1), time.Now().UnixNano())
	memory := float64(task.Mem)
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
		// port mapping only works in bridge mode
		dockerInfo.Network= mesosproto.ContainerInfo_DockerInfo_BRIDGE.Enum()
	} else if len(task.NetworkMode) > 0 {
		if task.NetworkMode == managerInterface.NETWORK_MODE_BRIDGE {
			dockerInfo.Network = mesosproto.ContainerInfo_DockerInfo_BRIDGE.Enum()
		} else if task.NetworkMode == managerInterface.NETWORK_MODE_HOST {
			dockerInfo.Network = mesosproto.ContainerInfo_DockerInfo_HOST.Enum()
		} else if task.NetworkMode == managerInterface.NETWORK_MODE_NONE {
			dockerInfo.Network = mesosproto.ContainerInfo_DockerInfo_NONE.Enum()
		}
	}


	commandInfo := &mesosproto.CommandInfo{
		Shell: proto.Bool(false),
	}
	if len(task.Arguments) > 0 {
		for _, argument := range task.Arguments {
			commandInfo.Arguments = append(commandInfo.Arguments, argument)
		}
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

func (self *manager) HandleDeleteTask(task *managerInterface.Task) {
	message := &mesosproto.KillTaskMessage{
		FrameworkId: &mesosproto.FrameworkID{Value: &self.frameworkId},
		TaskId: &mesosproto.TaskID{Value: &task.InternalID},
	}

	glog.Infof("Delete Task [%s] ", message)
	messagePackage := communication.NewMessage(self.masterUPID, message, nil)
	if err := communication.SendMessageToMesos(self.selfUPID, messagePackage); err != nil {
		glog.Errorf("Failed to send KillTaskMessage message: %v\n", err)
	}

}

func (self *manager) HandleReshuffleTasks() {
	tasks := self.GetRunningTasks()
	for _, task := range tasks {
		if task.Reshuffleable == false { continue }
		if task.Sla == managerInterface.SLA_ONE_PER_NODE { continue }

		self.HandleDeleteTask(task)
		newTask := managerInterface.CopyTask(*task)
		managerInterface.ResetTask(&newTask)
		self.HandleRunDockerImage(&newTask)
	}
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
