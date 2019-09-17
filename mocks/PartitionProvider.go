// Code generated by mockery v1.0.0. DO NOT EDIT.

package mocks

import io "io"
import mock "github.com/stretchr/testify/mock"
import util "github.com/mhelmich/calvin/util"

// PartitionProvider is an autogenerated mock type for the PartitionProvider type
type PartitionProvider struct {
	mock.Mock
}

// CreatePartition provides a mock function with given fields: partitionID
func (_m *PartitionProvider) CreatePartition(partitionID int) (util.DataStoreTxnProvider, error) {
	ret := _m.Called(partitionID)

	var r0 util.DataStoreTxnProvider
	if rf, ok := ret.Get(0).(func(int) util.DataStoreTxnProvider); ok {
		r0 = rf(partitionID)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(util.DataStoreTxnProvider)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(int) error); ok {
		r1 = rf(partitionID)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Snapshot provides a mock function with given fields: w
func (_m *PartitionProvider) Snapshot(w io.Writer) error {
	ret := _m.Called(w)

	var r0 error
	if rf, ok := ret.Get(0).(func(io.Writer) error); ok {
		r0 = rf(w)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}
