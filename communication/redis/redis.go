/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package redis

import (
	"time"
	"strconv"
	"net"
	"encoding/json"
	"fmt"

	"github.com/golang/glog"

	"github.com/att-innovate/charmander-scheduler/manager"
)

func InitRedisUpdater(manager manager.Manager) {
	go updateRedis(manager)
}

func updateRedis(manager manager.Manager) {
	for {
		glog.Infoln("update redis")
		if connection := redisAvailable(manager); connection != nil {
			for _, node := range manager.GetNodes() {
				nodeInJSON, _ := json.Marshal(&node)
				sendCommand(connection, "SET", "charmander:nodes:"+node.Hostname, fmt.Sprintf("%s", nodeInJSON))
				sendCommand(connection, "EXPIRE", "charmander:nodes:"+node.Hostname, "30") //timeout after 30s
			}
			connection.Close()
		}

		time.Sleep(15 * time.Second)
	}
}

func redisAvailable(manager manager.Manager) net.Conn {
	connection, error := net.DialTimeout("tcp", manager.GetRedisConnectionIPAndPort(), 2 * time.Second)
	if error != nil {
		return nil
	}

	return connection
}

func sendCommand(connection net.Conn, args ...string) {
	buffer := make([]byte, 0, 0)
	buffer = encodeReq(buffer, args)
	connection.Write(buffer)
}

func encodeReq(buf []byte, args []string) []byte {
	buf = append(buf, '*')
	buf = strconv.AppendUint(buf, uint64(len(args)), 10)
	buf = append(buf, '\r', '\n')
	for _, arg := range args {
		buf = append(buf, '$')
		buf = strconv.AppendUint(buf, uint64(len(arg)), 10)
		buf = append(buf, '\r', '\n')
		buf = append(buf, []byte(arg)...)
		buf = append(buf, '\r', '\n')
	}
	return buf
}


