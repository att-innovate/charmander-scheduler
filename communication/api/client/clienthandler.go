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

package client

import (
	"net/http"
	"fmt"
	"strings"
	"html"
	"encoding/json"

	"github.com/att/charmander-scheduler/manager"
)

type ClientHandler struct {
	Manager manager.Manager
}

func (self *ClientHandler) ServeHTTP(responseWriter http.ResponseWriter, request *http.Request) {
	path := html.EscapeString(request.URL.Path)
	if strings.HasSuffix(path, "/execute") {
		defer request.Body.Close()

		task := &manager.Task{}

		if err := json.NewDecoder(request.Body).Decode(&task); err != nil {
			self.writeError(responseWriter, http.StatusBadRequest, err.Error())
			return
		}

		if len(task.DockerImage) == 0 || len(task.ID) == 0 || task.Mem == 0 {
			self.writeError(responseWriter, http.StatusBadRequest, "docker_image and/or id and/or mem missing")
			return
		}

		self.Manager.HandleRunDockerImage(task)

		fmt.Fprintf(responseWriter, "done\n")
		return
	}
}

func (self *ClientHandler) writeError(responseWriter http.ResponseWriter, code int, message string) {
	responseWriter.WriteHeader(code)
	data := struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		}{
		code,
		message,
	}
	responseWriter.Header().Set("Content-Type", "application/json")
	json.NewEncoder(responseWriter).Encode(&data)
}


