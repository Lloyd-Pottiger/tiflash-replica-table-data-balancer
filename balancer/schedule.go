package balancer

import (
	"cmp"
	"slices"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type TiFlashStore struct {
	ID        int64
	RegionIDs []int64
}

func Schedule() error {
	tiflashStoreIDs, err := GetAllTiFlashStores(GlobalConfig.PDEndpoint)
	if err != nil {
		return err
	}
	if len(tiflashStoreIDs) < 2 {
		return errors.New("TiFlash stores less than 2")
	}
	startKey, endKey, err := GetTableKeyRange(GlobalConfig.PDEndpoint, GlobalConfig.TableID)
	if err != nil {
		return err
	}
	var tiflashStores []TiFlashStore
	totalRegionCount := 0
	for _, storeID := range tiflashStoreIDs {
		regionIDs, err := GetStoreRegionIDsInGivenRange(GlobalConfig.PDEndpoint, storeID, startKey, endKey)
		if err != nil {
			return err
		}
		totalRegionCount += len(regionIDs)
		tiflashStores = append(tiflashStores, TiFlashStore{ID: storeID, RegionIDs: regionIDs})
	}
	// sort TiFlash stores by region count in descending order
	slices.SortStableFunc(tiflashStores, func(lhs, rhs TiFlashStore) int {
		return -cmp.Compare(len(lhs.RegionIDs), len(rhs.RegionIDs))
	})
	// balance TiFlash stores
	for i := 0; i < len(tiflashStores)-1; i++ {
		for j := len(tiflashStores) - 1; j > i; j-- {
			if len(tiflashStores[j].RegionIDs) < totalRegionCount/len(tiflashStores) {
				regionID := tiflashStores[i].RegionIDs[len(tiflashStores[i].RegionIDs)-1]
				if err := AddTransferPeerOperator(GlobalConfig.PDEndpoint, regionID, tiflashStores[i].ID, tiflashStores[j].ID); err != nil {
					return err
				}
				log.Info("transfer peer", zap.Int64("region-id", regionID), zap.Int64("from-store", tiflashStores[i].ID), zap.Int64("to-store", tiflashStores[j].ID))
				tiflashStores[i].RegionIDs = tiflashStores[i].RegionIDs[:len(tiflashStores[i].RegionIDs)-1]
				tiflashStores[j].RegionIDs = append(tiflashStores[j].RegionIDs, regionID)
			} else {
				break
			}
		}
	}
	return nil
}
