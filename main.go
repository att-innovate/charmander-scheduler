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

package main

import (
	"flag"

	"github.com/golang/glog"

	managerImpl   "github.com/att-innovate/charmander-scheduler/manager/impl"
	schedulerImpl "github.com/att-innovate/charmander-scheduler/scheduler/impl"
)

// cmdline arguments
var masterAddress  = flag.String("master", "127.0.0.1:5050", "Address for Mesos Master")
var myIP           = flag.String("local-ip", "127.0.0.1", "Local IP Address")
var redisIPAndPort = flag.String("redis", "127.0.0.1:6379", "Redis IP Address:Port")


func main() {
	flag.Parse()

	glog.Infoln("Starting charmander-scheduler.")

	myManager, err := managerImpl.New(schedulerImpl.Scheduler, "Charmander Scheduler", *masterAddress, *myIP, *redisIPAndPort)
	if err != nil {
		glog.Errorln("Unable to create a Manager ", err.Error())
		return
	}

	err = myManager.Start()
	if err != nil {
		glog.Errorln("Unable to start Manager: ", err.Error())
		return
	}

	exit := make(chan bool)
	<-exit
}
