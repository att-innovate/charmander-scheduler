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

package mesos

import (
	"fmt"
	"net/http"
	"strings"
	"io/ioutil"

	"code.google.com/p/gogoprotobuf/proto"
	"github.com/golang/glog"

	"github.com/att-innovate/charmander-scheduler/manager"
	"github.com/att-innovate/charmander-scheduler/mesosproto"
)

type MesosHandler struct {
	Manager manager.Manager
}

func (self *MesosHandler) ServeHTTP(responseWriter http.ResponseWriter, request *http.Request) {
	err := verifyLibprocessRequest(request)
	if err != nil {
		glog.Errorf("Ignoring the request: %v\n", err)
		responseWriter.WriteHeader(http.StatusBadRequest)

		return
	}

	data, err := ioutil.ReadAll(request.Body)
	if err != nil {
		glog.Errorf("Failed to read HTTP body: %v\n", err)
		responseWriter.WriteHeader(http.StatusBadRequest)
		return
	}

	responseWriter.WriteHeader(http.StatusAccepted)

	messageName := extractNameFromRequestURI(request.RequestURI)
	glog.Infof("Message name %v\n", messageName)

	switch{
	case messageName == "mesos.internal.FrameworkRegisteredMessage" :
		registeredMessage := &mesosproto.FrameworkRegisteredMessage{}
		if err := proto.Unmarshal(data, registeredMessage); err != nil {
			glog.Errorf("Failed to unmarshal message %v\n", messageName)
			return
		}
		self.Manager.HandleFrameworkRegistered(registeredMessage.FrameworkId.GetValue())
	case messageName == "mesos.internal.ResourceOffersMessage":
		offeredMessage := &mesosproto.ResourceOffersMessage{}
		if err := proto.Unmarshal(data, offeredMessage); err != nil {
			glog.Errorf("Failed to unmarshal message %v\n", messageName)
			return
		}
		self.Manager.HandleResourceOffered(offeredMessage.GetOffers())
	case messageName == "mesos.internal.StatusUpdateMessage":
		statusMessage := &mesosproto.StatusUpdateMessage{}
		if err := proto.Unmarshal(data, statusMessage); err != nil {
			glog.Errorf("Failed to unmarshal message %v\n", messageName)
			return
		}
		self.Manager.HandleStatusMessage(statusMessage)
	}

}

func verifyLibprocessRequest(r *http.Request) error {
	if r.Method != "POST" {
		return fmt.Errorf("Not a libprocess POST request")
	}
	ua, ok := r.Header["User-Agent"]
	if ok && strings.HasPrefix(ua[0], "libprocess/") {
		return nil
	}
	_, ok = r.Header["Libprocess-From"]
	if ok {
		return nil
	}
	return fmt.Errorf("Cannot find 'User-Agent' or 'Libprocess-From'")
}

func extractNameFromRequestURI(requestURI string) string {
	return strings.Split(requestURI, "/")[2]
}
