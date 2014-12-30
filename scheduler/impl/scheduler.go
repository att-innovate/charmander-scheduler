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
	"strconv"
	"math"

	"github.com/golang/glog"

	"github.com/att-innovate/charmander-scheduler/scheduler"
	"github.com/att-innovate/charmander-scheduler/mesosproto"

	managerInterface "github.com/att-innovate/charmander-scheduler/manager"
)

// my scheduler
var Scheduler = &scheduler.Scheduler{}

const (
	MBDivisor = 1000000
	TwoMB = 2
)

func init() {
	glog.Infoln("Initializing scheduler ...")

	Scheduler.Registered = func(manager managerInterface.Manager, frameworkId string) {
		glog.Infoln("Registered FrameworkId ", frameworkId)
	}

	Scheduler.OverwriteTaskAttributes = func(manager managerInterface.Manager, taskRequest *managerInterface.Task) {
		memObservedRaw := manager.GetTaskIntelligence(taskRequest.ID, "mem")
		if len(memObservedRaw) > 0 {
			memObserved, _ := strconv.Atoi(memObservedRaw)
			if memObserved < MBDivisor {
				taskRequest.Mem = uint64(TwoMB)
			} else {
				taskRequest.Mem = uint64((math.Ceil(float64(memObserved / MBDivisor) * 1.1))) // add a 10% safety net
			}
		}
	}

	Scheduler.ResourceOffers = func(manager managerInterface.Manager, offers []*mesosproto.Offer) {
		var taskRequests []*managerInterface.Task
		taskRequests = manager.GetOpenTaskRequests()

		glog.Infoln("Got ", len(offers), "offer(s) from master.")

		for _, offer := range offers {
			matchFound := false

			for _, taskRequest := range taskRequests {
				if matchFound { break }
				if !manager.ResourceRequirementsWouldMatch(offer, taskRequest) { continue }

				manager.AcceptOffer(offer.GetId(), offer.SlaveId, taskRequest)
				matchFound = true
			}

			if !matchFound {
				manager.DeclineOffer(offer.GetId())
			}
		}
	}

}
