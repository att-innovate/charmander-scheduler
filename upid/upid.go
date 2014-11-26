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

package upid

import (
	"fmt"
	"net"
	"strings"
)

// UPID is a equivalent of the UPID in libprocess.
type UPID struct {
	ID   string
	Host string
	Port string
}

// Parse parses the UPID from the input string.
func Parse(input string) (*UPID, error) {
	upid := new(UPID)

	splits := strings.Split(input, "@")
	if len(splits) != 2 {
		return nil, fmt.Errorf("Expect one `@' in the input")
	}
	upid.ID = splits[0]

	if _, err := net.ResolveTCPAddr("tcp4", splits[1]); err != nil {
		return nil, err
	}
	upid.Host, upid.Port, _ = net.SplitHostPort(splits[1])
	return upid, nil
}

// String returns the string representation.
func (u *UPID) String() string {
	return fmt.Sprintf("%s@%s:%s", u.ID, u.Host, u.Port)
}

// Equal returns true if two upid is equal
func (u *UPID) Equal(upid *UPID) bool {
	if u.ID == upid.ID && u.Host == upid.Host && u.Port == upid.Port {
		return true
	}
	return false
}
