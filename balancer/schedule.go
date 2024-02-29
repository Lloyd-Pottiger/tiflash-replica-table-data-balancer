package balancer

import (
	"cmp"
	"encoding/hex"
	"slices"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"

	client "github.com/Lloyd-Pottiger/tiflash-replica-table-data-balancer/pd_client"
)

type TiFlashStore struct {
	ID          int64
	RegionIDs   []int64
	RegionIDSet map[int64]struct{}
}

func InitTiFlashStore(id int64, regionIDs []int64) TiFlashStore {
	regionIDSet := make(map[int64]struct{})
	for _, regionID := range regionIDs {
		regionIDSet[regionID] = struct{}{}
	}
	return TiFlashStore{ID: id, RegionIDs: regionIDs, RegionIDSet: regionIDSet}
}

func Schedule(pd client.PDClient, tableID int64, zone, region string) error {
	tiflashStoreIDs, err := pd.GetAllTiFlashStores(zone, region)
	if err != nil {
		return err
	}
	if len(tiflashStoreIDs) < 2 {
		return errors.New("TiFlash stores less than 2")
	}
	log.Info("TiFlash stores", zap.Any("store-ids", tiflashStoreIDs))
	startKey, endKey, err := pd.GetTableKeyRange(tableID)
	if err != nil {
		return err
	}
	log.Info("Key range for table", zap.Int64("table-id", tableID), zap.String("start-key", hex.EncodeToString(startKey)), zap.String("end-key", hex.EncodeToString(endKey)))
	var tiflashStores []TiFlashStore
	totalRegionCount := 0
	for _, storeID := range tiflashStoreIDs {
		regionIDs, err := pd.GetStoreRegionIDsInGivenRange(storeID, startKey, endKey)
		if err != nil {
			return err
		}
		log.Info("store region", zap.Int64("store-id", storeID), zap.Any("region", regionIDs))
		totalRegionCount += len(regionIDs)
		tiflashStores = append(tiflashStores, InitTiFlashStore(storeID, regionIDs))
	}
	// sort TiFlash stores by region count in descending order
	slices.SortStableFunc(tiflashStores, func(lhs, rhs TiFlashStore) int {
		return -cmp.Compare(len(lhs.RegionIDs), len(rhs.RegionIDs))
	})
	// balance TiFlash stores
	// TODO: limit the number of transfer peer operators
	for i := 0; i < len(tiflashStores)-1; i++ {
		for j := len(tiflashStores) - 1; j > i; j-- {
			fromStore := &tiflashStores[i]
			toStore := &tiflashStores[j]
			for len(fromStore.RegionIDs) > totalRegionCount/len(tiflashStores) && len(toStore.RegionIDs) < totalRegionCount/len(tiflashStores) {
				regionID := fromStore.RegionIDs[len(fromStore.RegionIDs)-1]
				// check if the region is already in the target store
				if _, exist := toStore.RegionIDSet[regionID]; exist {
					continue
				}
				if err := pd.AddTransferPeerOperator(regionID, fromStore.ID, toStore.ID); err != nil {
					return err
				}
				log.Info("transfer peer", zap.Int64("region-id", regionID), zap.Int64("from-store", fromStore.ID), zap.Int64("to-store", toStore.ID))
				fromStore.RegionIDs = fromStore.RegionIDs[:len(fromStore.RegionIDs)-1]
				toStore.RegionIDs = append(toStore.RegionIDs, regionID)
			}
		}
	}
	return nil
}
