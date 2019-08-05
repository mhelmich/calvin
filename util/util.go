/*
 * Copyright 2018 Marco Helmich
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package util

import (
	"crypto/rand"
	"encoding/binary"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
)

// converts bytes to an unsinged 64 bit integer
func BytesToUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

// converts a uint64 to a byte slice
func Uint64ToBytes(u uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, u)
	return buf
}

func Uint64ToBytesInto(u uint64, buf []byte) error {
	binary.BigEndian.PutUint64(buf, u)
	return nil
}

func Uint64ToString(i uint64) string {
	return strconv.FormatUint(i, 10)
}

func stringToUint64(s string) uint64 {
	i, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		log.Panicf("Can't prase string %s to uint64: %s", s, err.Error())
	}
	return i
}

func RandomRaftId() uint64 {
	bites := make([]byte, 8)
	_, err := rand.Read(bites)
	if err != nil {
		log.Panicf("Can't read from random: %s", err.Error())
	}
	return BytesToUint64(bites)
}

func NowUnixUtc() uint64 {
	t := time.Now().UTC()
	return uint64(t.Unix())*1000 + uint64(t.Nanosecond()/int(time.Millisecond))
}

func IsSyncMapEmpty(m *sync.Map) bool {
	isEmpty := true
	m.Range(func(key, value interface{}) bool {
		// if we ever execute this,
		// the map isn't empty
		isEmpty = false
		return false
	})
	return isEmpty
}

// get preferred outbound ip of this machine
func OutboundIP() net.IP {
	// google dns
	// the address may not exist
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		logrus.Panicf("%s\n", err.Error())
	}
	defer conn.Close()
	localAddr := conn.LocalAddr().(*net.UDPAddr)
	// get the IP from an open connection
	return localAddr.IP
}
