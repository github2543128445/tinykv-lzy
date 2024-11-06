// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"sort"

	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("balance-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	balanceRegionName       = "balance-region-scheduler"
)

type balanceRegionScheduler struct {
	*baseScheduler
	name         string
	opController *schedule.OperatorController
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return balanceRegionName
}

func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	// Your Code Here (3C).
	candidateStores := []*core.StoreInfo{}
	//找到符合条件的stores
	for _, store := range cluster.GetStores() {
		if store.IsUp() && store.DownTime() <= cluster.GetMaxStoreDownTime() {
			candidateStores = append(candidateStores, store)
		}
	}
	if len(candidateStores) < 2 {
		return nil
	}
	sort.Slice(candidateStores, func(i, j int) bool {
		return candidateStores[i].GetRegionSize() > candidateStores[j].GetRegionSize()
	})

	var moveRegion *core.RegionInfo
	var sourceStore, desStore *core.StoreInfo
	for _, store := range candidateStores {
		cluster.GetPendingRegionsWithLock(store.GetID(), func(container core.RegionsContainer) {
			moveRegion = container.RandomRegion(nil, nil)
		})
		if moveRegion != nil {
			sourceStore = store
			break
		}
		cluster.GetFollowersWithLock(store.GetID(), func(container core.RegionsContainer) {
			moveRegion = container.RandomRegion(nil, nil)
		})
		if moveRegion != nil {
			sourceStore = store
			break
		}
		cluster.GetLeadersWithLock(store.GetID(), func(container core.RegionsContainer) {
			moveRegion = container.RandomRegion(nil, nil)
		})
		if moveRegion != nil {
			sourceStore = store
			break
		}
	}
	if moveRegion == nil {
		return nil
	}
	if len(moveRegion.GetStoreIds()) < cluster.GetMaxReplicas() {
		return nil
	}

	for i := len(candidateStores) - 1; i >= 0; i-- {
		store := candidateStores[i]
		exist := moveRegion.GetStorePeer(store.GetID())
		if exist == nil {
			desStore = store
			break
		}
	}
	if desStore == nil {
		return nil
	}
	if sourceStore.GetRegionSize()-desStore.GetRegionSize() <= 2*moveRegion.GetApproximateSize() {
		return nil
	}

	newPeer, err := cluster.AllocPeer(desStore.GetID())
	if err != nil {
		panic(err)
	}
	op, err := operator.CreateMovePeerOperator("balance_region", cluster, moveRegion, operator.OpBalance, sourceStore.GetID(), desStore.GetID(), newPeer.GetId())
	if err != nil {
		panic(err)
	}
	return op
}
