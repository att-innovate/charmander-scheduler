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

	"github.com/att-innovate/charmander-scheduler/manager"
)

type ClientHandler struct {
	Manager manager.Manager
}

func (self *ClientHandler) ServeHTTP(responseWriter http.ResponseWriter, request *http.Request) {
	path := html.EscapeString(request.URL.Path)
	if strings.HasSuffix(path, "/task") && request.Method == "POST"  {
		defer request.Body.Close()

		task := &manager.Task{}

		if err := json.NewDecoder(request.Body).Decode(&task); err != nil {
			self.writeResponse(responseWriter, http.StatusBadRequest, err.Error())
			return
		}

		if len(task.DockerImage) == 0 || len(task.ID) == 0 || task.Mem == 0 {
			self.writeResponse(responseWriter, http.StatusBadRequest, "docker_image and/or id and/or mem missing")
			return
		}

		self.Manager.HandleRunDockerImage(task)

		self.writeResponse(responseWriter, http.StatusAccepted, fmt.Sprintf("Run task: %s", task.ID))
		return

	} else if strings.HasSuffix(path, "/task/reshuffle") && request.Method == "GET" {
		self.Manager.HandleReshuffleTasks()
		self.writeResponse(responseWriter, http.StatusAccepted, "Reshuffle tasks")
		return
	} else if strings.Contains(path, "/task/") && request.Method == "DELETE" {
		pathElmts := strings.Split(path, "/")
		foundATask := false
		if len(pathElmts) == 4 && len(pathElmts[3]) > 0 {
			tasks := self.Manager.GetTasks()
			for _, task := range tasks {
				if strings.HasPrefix(task.InternalID,  pathElmts[3]) {
					self.Manager.HandleDeleteTask(task)
					self.writeResponse(responseWriter, http.StatusOK, fmt.Sprintf("Kill task: %s", task.InternalID))
					foundATask = true
				}
			}

			if foundATask { return }
		}
	}

	self.writeResponse(responseWriter, http.StatusBadRequest, "incorrect request")
}

func (self *ClientHandler) writeResponse(responseWriter http.ResponseWriter, code int, message string) {
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


