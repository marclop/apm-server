// Licensed to Elasticsearch B.V. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Elasticsearch B.V. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package v2

import (
	fmt "fmt"

	"google.golang.org/grpc/encoding"
	protoname "google.golang.org/grpc/encoding/proto"
	"google.golang.org/protobuf/proto"
)

func init() {
	// Use more efficient encoding when available.
	encoding.RegisterCodec(codec{})
}

type codec struct{}

type vtprotoMessage interface {
	MarshalVT() ([]byte, error)
	UnmarshalVT([]byte) error
}

// Marshal returns the wire format of v.
func (c codec) Marshal(v interface{}) ([]byte, error) {
	if vt, ok := v.(vtprotoMessage); ok {
		return vt.MarshalVT()
	}
	if msg, ok := v.(proto.Message); ok {
		return proto.Marshal(msg)
	}
	return nil, fmt.Errorf("failed to marshal, message is %T, want proto.Message", v)
}

// Unmarshal parses the wire format into v.
func (c codec) Unmarshal(data []byte, v interface{}) error {
	if vt, ok := v.(vtprotoMessage); ok {
		return vt.UnmarshalVT(data)
	}
	if msg, ok := v.(proto.Message); ok {
		return proto.Unmarshal(data, msg)
	}
	return fmt.Errorf("failed to unmarshal, message is %T, want proto.Message", v)
}

// Name returns the name of the proto encoding, use the same value as the
// encoding/proto package.
func (c codec) Name() string { return protoname.Name }
