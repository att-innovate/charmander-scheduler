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
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/att/charmander-scheduler/manager"

	"github.com/golang/glog"

	adminapi "github.com/att/charmander-scheduler/communication/api/admin"
	clientapi "github.com/att/charmander-scheduler/communication/api/client"
	mesosapi "github.com/att/charmander-scheduler/communication/api/mesos"
)

func InitRestHandler(manager manager.Manager) error {

	go func() {
		glog.Infof("SCHEDULER listening at: http://%v:%v", manager.GetListenerIP(), manager.GetListenerPortForScheduler())

		listener, err := net.Listen("tcp4", fmt.Sprintf("%v:%v", manager.GetListenerIP(), manager.GetListenerPortForScheduler()))
		if err != nil {
			glog.Errorf("HTTPTransporter failed to listen: %v\n", err)
			return
		}

		mux := http.NewServeMux()
		mux.Handle("/scheduler/", &mesosapi.MesosHandler{Manager: manager})

		http.Serve(listener, mux)
	}()

	go func() {
		glog.Infof("REST listening at: http://%v:%v", manager.GetListenerIP(), manager.GetListenerPortForRESTApi())

		mux := http.NewServeMux()
		mux.Handle("/admin/", &adminapi.AdminHandler{})
		mux.Handle("/client/", &clientapi.ClientHandler{Manager: manager})

		s := &http.Server{
			Addr:           fmt.Sprintf(":%v", manager.GetListenerPortForRESTApi()),
			Handler:        mux,
			ReadTimeout:    10 * time.Second,
			WriteTimeout:   10 * time.Second,
			MaxHeaderBytes: 1 << 20,
		}

		s.ListenAndServe()
	}()

	return nil
}
