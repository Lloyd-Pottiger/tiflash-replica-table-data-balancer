package balancer

import (
	"cmp"
	"encoding/hex"
	"fmt"
	"slices"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"

	client "github.com/Lloyd-Pottiger/tiflash-replica-table-data-balancer/pd_client"
)

type TiFlashStore struct {
	ID          int64
	RegionIDSet map[int64]struct{}
}

func InitTiFlashStore(id int64, regionIDs []int64) TiFlashStore {
	regionIDSet := make(map[int64]struct{})
	for _, regionID := range regionIDs {
		regionIDSet[regionID] = struct{}{}
	}
	return TiFlashStore{ID: id, RegionIDSet: regionIDSet}
}

func Schedule(pd client.PDClient, tableID int64, zone, region string, dryRun, showOnly bool) error {
	tiflashStoreIDs, err := pd.GetAllTiFlashStores(zone, region)
	if err != nil {
		return err
	}
	if len(tiflashStoreIDs) < 2 {
		return errors.New("TiFlash stores less than 2")
	}
	log.Info("schedule run", zap.String("zone", zone), zap.String("region", region), zap.Bool("dry-run", dryRun), zap.Bool("show-only", showOnly))
	if dryRun {
		log.Info("Schedule running in dry-run mode, it will only print the operator commands. If you want to send the operators to PD, add --dry-run=false")
	}
	log.Info("TiFlash stores", zap.String("zone", zone), zap.String("region", region), zap.Int("num-store", len(tiflashStoreIDs)), zap.Any("store-ids", tiflashStoreIDs))
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
		log.Info("store region", zap.Int64("store-id", storeID), zap.Int("num-region", len(regionIDs)), zap.Any("region", regionIDs))
		totalRegionCount += len(regionIDs)
		tiflashStores = append(tiflashStores, InitTiFlashStore(storeID, regionIDs))
	}
	expectedRegionCountPerStore := totalRegionCount / len(tiflashStores)
	log.Info("total region peer count", zap.Int("total-num-region-peer", totalRegionCount), zap.Int("expect-num-region-per-store", expectedRegionCountPerStore))
	for _, store := range tiflashStores {
		log.Info("store region dist",
			zap.Int64("store-id", store.ID),
			zap.Int("num-region", len(store.RegionIDSet)),
			zap.Float64("percentage", float64(len(store.RegionIDSet))/float64(totalRegionCount)))
	}
	if showOnly {
		// only show the region distribution
		return nil
	}

	// sort TiFlash stores by region count in descending order
	slices.SortStableFunc(tiflashStores, func(lhs, rhs TiFlashStore) int {
		return -cmp.Compare(len(lhs.RegionIDSet), len(rhs.RegionIDSet))
	})
	// balance TiFlash stores
	// TODO: limit the number of transfer peer operators
	log.Info("balance begin")
	for i := 0; i < len(tiflashStores)-1; i++ {
		for j := len(tiflashStores) - 1; j > i; j-- {
			fromStore := &tiflashStores[i]
			toStore := &tiflashStores[j]
			fromStoreRegionSet := &fromStore.RegionIDSet
			toStoreRegionSet := &toStore.RegionIDSet
			numRegionsFromBeg, numRegionsToBeg := len(*fromStoreRegionSet), len(*toStoreRegionSet)
			numOperatorGen := 0
			log.Info("checking transfer peer",
				zap.Int64("from-store", fromStore.ID), zap.Int64("to-store", toStore.ID),
				zap.Int("num-from-regions-beg", numRegionsFromBeg),
				zap.Int("num-to-regions-beg", numRegionsToBeg))
			for regionID := range *fromStoreRegionSet {
				if len(*fromStoreRegionSet) <= expectedRegionCountPerStore || len(*toStoreRegionSet) >= expectedRegionCountPerStore {
					break
				}
				if _, exist := (*toStoreRegionSet)[regionID]; exist {
					// If the region is already in the target store, skip it
					continue
				}
				if dryRun {
					log.Info(fmt.Sprintf("operator add transfer-peer %d %d %d", regionID, fromStore.ID, toStore.ID))
				} else {
					log.Info("transfer peer", zap.Int64("region-id", regionID), zap.Int64("from-store", fromStore.ID), zap.Int64("to-store", toStore.ID))
					if err := pd.AddTransferPeerOperator(regionID, fromStore.ID, toStore.ID); err != nil {
						return err
					}
				}
				delete(*fromStoreRegionSet, regionID)
				(*toStoreRegionSet)[regionID] = struct{}{}
				numOperatorGen += 1
			}
			numRegionsFromEnd, numRegionsToEnd := len(*fromStoreRegionSet), len(*toStoreRegionSet)
			log.Info("generate transfer peer",
				zap.Int64("from-store", fromStore.ID), zap.Int64("to-store", toStore.ID),
				zap.Int("num-from-regions-beg", numRegionsFromBeg), zap.Int("num-from-regions-end", numRegionsFromEnd),
				zap.Int("num-to-regions-beg", numRegionsToBeg), zap.Int("num-to-regions-end", numRegionsToEnd),
				zap.Int("total", numOperatorGen))
		}
	}
	log.Info("balance end")
	return nil
}
