package balancer

import (
	client "github.com/Lloyd-Pottiger/tiflash-replica-table-data-balancer/pd_client"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"

	pdhttp "github.com/tikv/pd/client/http"
)

func GetRegionsWithTiFlashReplica(pd client.PDClient, tiflashStoreIds map[int64]int) ([]pdhttp.RegionInfo, error) {
	var result []pdhttp.RegionInfo
	regions, e := pd.GetRegions()
	if e != nil {
		return nil, e
	}
	for _, region := range regions {
		hasReplica := false
		for _, peer := range region.Peers {
			_, ok := tiflashStoreIds[peer.StoreID]
			if ok {
				hasReplica = true
				break
			}
		}
		if hasReplica {
			result = append(result, region)
		}
	}
	return result, nil
}

func GetRegionsWithTiFlashReplicaLiveness(regions []pdhttp.RegionInfo, tiflashStoreIdMap map[int64]int, offlineStoreIdMap map[int64]int) ([]pdhttp.RegionInfo, []pdhttp.RegionInfo) {
	var resultAlive []pdhttp.RegionInfo
	var resultDead []pdhttp.RegionInfo
	for _, region := range regions {
		alive := false
		for _, peer := range region.Peers {
			_, isTiFlash := tiflashStoreIdMap[peer.StoreID]
			_, isOffline := offlineStoreIdMap[peer.StoreID]
			if isTiFlash {
				if !isOffline {
					alive = true
					break
				}
			}
		}
		if !alive {
			resultDead = append(resultDead, region)
		} else {
			resultAlive = append(resultAlive, region)
		}
	}
	return resultAlive, resultDead
}

func LocationLabelMatch(stores map[int64]pdhttp.StoreInfo, candidate *pdhttp.StoreInfo, offlineStores []int64) bool {
	candidateRegion := ""
	candidateZone := ""
	for _, label := range candidate.Store.Labels {
		if label.Key == "region" {
			candidateRegion = label.Value
		}
		if label.Key == "zone" {
			candidateZone = label.Value
		}
	}
	for _, offlineId := range offlineStores {
		offlineStore, ok := stores[offlineId]
		if !ok {
			continue
		}
		totallyMatch := true
		for _, label := range offlineStore.Store.Labels {
			if label.Key == "region" && label.Value != candidateRegion {
				totallyMatch = false
				break
			}
			if label.Key == "zone" && label.Value != candidateZone {
				totallyMatch = false
				break
			}
		}
		if totallyMatch {
			return true
		}
	}
	return false
}

func PickOneTiFlashStore(region *pdhttp.RegionInfo, stores map[int64]pdhttp.StoreInfo, offlineStoreIdMap map[int64]int, offlineStoreIds []int64) *pdhttp.StoreInfo {
	var candidate *pdhttp.StoreInfo
	candidate = nil
	for storeID, storeInfo := range stores {
		alreadyHasPeer := false
		for _, p := range region.Peers {
			if p.StoreID == storeID {
				alreadyHasPeer = true
				break
			}
		}
		_, isOffline := offlineStoreIdMap[storeID]
		if !isOffline && !alreadyHasPeer {
			// Perfect match directly return
			if LocationLabelMatch(stores, &storeInfo, offlineStoreIds) {
				return &storeInfo
			} else {
				candidate = &storeInfo
			}
		}
	}
	return candidate
}

func ScheduleRegion(pd client.PDClient, offlineStoreIds []int64, offline, dryRun bool) error {
	tiflashStoreIDs, tiflashStoreMap, err := pd.GetAllTiFlashStores("", "")
	if err != nil {
		return err
	}
	if len(tiflashStoreIDs) < 2 {
		return errors.New("TiFlash stores less than 2")
	}
	log.Info("region schedule run", zap.Any("offlineStoreIds", offlineStoreIds), zap.Bool("dry-run", dryRun))
	if dryRun {
		log.Info("region schedule running in dry-run mode, it will only print the operator commands. If you want to send the operators to PD, add --dry-run=false")
	}
	tiflashStoreIdMap := make(map[int64]int)
	for _, v := range tiflashStoreIDs {
		tiflashStoreIdMap[v] = 1
	}
	offlineStoreIdMap := make(map[int64]int)
	for _, v := range offlineStoreIds {
		offlineStoreIdMap[v] = 1
	}

	tiflashRegions, err := GetRegionsWithTiFlashReplica(pd, tiflashStoreIdMap)
	if err != nil {
		return err
	}

	tiflashDeadRegions, tiflashAliveRegions := GetRegionsWithTiFlashReplicaLiveness(tiflashRegions, tiflashStoreIdMap, offlineStoreIdMap)
	var tiflashDeadRegionIds []int64
	var tiflashAliveRegionIds []int64

	for _, region := range tiflashDeadRegions {
		tiflashDeadRegionIds = append(tiflashDeadRegionIds, region.ID)
	}
	for _, region := range tiflashAliveRegions {
		tiflashAliveRegionIds = append(tiflashAliveRegionIds, region.ID)
	}

	log.Info("TiFlash region schedule result",
		zap.Any("tiflashStoreIDs", tiflashStoreIDs),
		zap.Any("offlineStoreIds", offlineStoreIds),
		zap.Any("tiflashDeadRegionIds", tiflashDeadRegionIds),
		zap.Any("tiflashAliveRegions", tiflashAliveRegions),
		zap.Any("tiflashAliveRegionIds", tiflashAliveRegionIds))

	if offline {
		for _, v := range offlineStoreIds {
			log.Info("Delete offline store id",
				zap.Any("storeID", v))
			if !dryRun {
				pd.DeleteStore(v)
			}
		}
	}

	for _, region := range tiflashDeadRegions {
		pickStore := PickOneTiFlashStore(&region, tiflashStoreMap, offlineStoreIdMap, offlineStoreIds)
		if pickStore == nil {
			log.Warn("Failed to find another store to place this region",
				zap.Any("region", region))
			continue
		}
		log.Info("Found another store to place this region",
			zap.Any("storeID", pickStore.Store.ID),
			zap.Any("region", region))
		if !dryRun {
			pd.AddCreatePeerOperator(region.ID, pickStore.Store.ID)
		}
	}

	return nil
}
