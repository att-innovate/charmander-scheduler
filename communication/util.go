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

package communication

import (
	"bytes"
	"fmt"
	"net"
	"net/http"

	"github.com/golang/glog"
	"code.google.com/p/gogoprotobuf/proto"

	"github.com/att-innovate/charmander-scheduler/upid"
)

func MesosMasterReachable(masterAddress string) bool {
	_, err := http.Get("http://" + masterAddress + "/health")
	if err != nil {
		glog.Errorf("Failed to connect to mesos %v Error: %v\n", masterAddress, err)
		return false
	}

	return true
}

func SendMessageToMesos(sender *upid.UPID, msg *Message) error {
	glog.Infof("Sending message to %v from %v\n", msg.UPID, sender)
	// marshal payload
	b, err := proto.Marshal(msg.ProtoMessage)
	if err != nil {
		glog.Errorf("Failed to marshal message %v: %v\n", msg, err)
		return err
	}
	msg.Bytes = b
	// create request
	req, err := makeLibprocessRequest(sender, msg)
	if err != nil {
		glog.Errorf("Failed to make libprocess request: %v\n", err)
		return err
	}
	// send it
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		glog.Errorf("Failed to POST: %v %v\n", err, resp)
		return err
	}
	resp.Body.Close()
	// ensure master acknowledgement.
	if (resp.StatusCode != http.StatusOK) &&
		(resp.StatusCode != http.StatusAccepted) {
		msg := fmt.Sprintf("Master %s rejected %s.  Returned status %s.", msg.UPID, msg.RequestURI(), resp.Status)
		glog.Errorln(msg)
		return fmt.Errorf(msg)
	}

	return nil
}

func makeLibprocessRequest(sender *upid.UPID, msg *Message) (*http.Request, error) {
	hostport := net.JoinHostPort(msg.UPID.Host, msg.UPID.Port)
	targetURL := fmt.Sprintf("http://%s%s", hostport, msg.RequestURI())
	glog.Infof("Target URL %s", targetURL)
	req, err := http.NewRequest("POST", targetURL, bytes.NewReader(msg.Bytes))
	if err != nil {
		glog.Errorf("Failed to create request: %v\n", err)
		return nil, err
	}
	req.Header.Add("Libprocess-From", sender.String())
	req.Header.Add("Content-Type", "application/x-protobuf")
	req.Header.Add("Connection", "Keep-Alive")

	return req, nil
}

