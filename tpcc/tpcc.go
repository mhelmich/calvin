/*
 * Copyright 2019 Marco Helmich
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

package main

import (
	"fmt"
	"math/rand"

	"github.com/mhelmich/calvin"
	calvinpb "github.com/mhelmich/calvin/pb"
	"github.com/mhelmich/calvin/tpcc/pb"
)

const (
	numWarehouses         = 10
	districtsPerWarehouse = 15
	customersPerDistrict  = 30
	numItems              = 100
)

var warehouses = make(map[string]*pb.Warehouse)
var warehouseIDs = make([]string, numWarehouses)

func initDatastore() []*calvinpb.Transaction {
	txns := make([]*calvinpb.Transaction, 0)
	for i := 0; i < numWarehouses; i++ {
		txn := calvin.NewTransaction()
		txn.StoredProcedure = "__simple_setter__"

		warehouseID := fmt.Sprintf("%x", i)
		warehouse := createWarehouse(warehouseID)
		warehouses[warehouseID] = warehouse
		warehouseIDs[i] = warehouseID
		warehouseBites, _ := warehouse.Marshal()
		txn.AddSimpleSetterArg([]byte(warehouseID), warehouseBites)

		for j := 0; j < districtsPerWarehouse; j++ {
			districtID := fmt.Sprintf("w%dd%d", i, j)
			district := createDistrict(districtID, warehouseID)
			districtBites, _ := district.Marshal()
			txn.AddSimpleSetterArg([]byte(districtID), districtBites)

			for k := 0; k < customersPerDistrict; k++ {
				customerID := fmt.Sprintf("w%dd%dc%d", i, j, k)
				customer := createCustomer(customerID, warehouseID, districtID)
				customerBites, _ := customer.Marshal()
				txn.AddSimpleSetterArg([]byte(customerID), customerBites)
			}
		}

		for j := 0; j < numItems; j++ {
			itemID := fmt.Sprintf("i%d", j)
			ID := fmt.Sprintf("%ss%s", warehouseID, itemID)
			item := createStock(ID, itemID, warehouseID)
			itemBites, _ := item.Marshal()
			txn.AddSimpleSetterArg([]byte(ID), itemBites)
		}

		txns = append(txns, txn)
	}

	// for i := 0; i < numItems; i++ {
	// }

	return txns
}

func createWarehouse(ID string) *pb.Warehouse {
	return &pb.Warehouse{
		Id:         ID,
		Name:       randomString(20),
		Street_1:   randomString(20),
		Street_2:   randomString(20),
		City:       randomString(20),
		State:      randomString(2),
		Zip:        randomString(5),
		Tax:        float64(0.05),
		YearToDate: float64(0),
	}
}

func createDistrict(ID string, warehouseID string) *pb.District {
	return &pb.District{
		Id:          ID,
		WarehouseId: warehouseID,
		Name:        randomString(20),
		Street_1:    randomString(20),
		Street_2:    randomString(20),
		City:        randomString(20),
		State:       randomString(2),
		Zip:         randomString(5),
		Tax:         float64(0.05),
		YearToDate:  float64(0),
		NextOrderId: 1,
	}
}

func createCustomer(ID string, warehouseID string, districtID string) *pb.Customer {
	return &pb.Customer{
		Id:                ID,
		WarehouseId:       warehouseID,
		DistrictId:        districtID,
		First:             randomString(20),
		Middle:            randomString(20),
		Last:              ID,
		Street_1:          randomString(20),
		Street_2:          randomString(20),
		City:              randomString(20),
		State:             randomString(2),
		Zip:               randomString(5),
		Since:             int32(0),
		Credit:            "GC",
		CreditLimit:       float64(0.01),
		Discount:          float64(0.5),
		Balance:           float64(0),
		YearToDatePayment: float64(0),
		PaymentCount:      int32(3),
		DeliveryCount:     int32(3),
		Data:              []byte(randomString(50)),
	}
}

func createStock(ID string, itemID string, warehouseID string) *pb.Stock {
	return &pb.Stock{
		Id:          ID,
		ItemId:      itemID,
		WarehouseId: warehouseID,
		YearToDate:  int32(0),
		OrderCount:  int32(0),
		RemoteCount: int32(0),
		Data:        []byte(randomString(50)),
	}
}

func createItem(ID string) *pb.Item {
	return &pb.Item{
		Id:    ID,
		Name:  randomString(20),
		Price: rand.Float64(),
		Data:  []byte(randomString(50)),
	}
}
