package local

import (
	"encoding/json"
	"fmt"
	"os"

	tidbcodec "github.com/JaySon-Huang/tiflash-ctl/pkg/tidb"
	client "github.com/Lloyd-Pottiger/tiflash-replica-table-data-balancer/pd_client"
	"github.com/pingcap/errors"
	pdhttp "github.com/tikv/pd/client/http"
)

type LocalClient struct {
	StoresFile   string
	RegionsFiles []string
}

func (pd *LocalClient) AddTransferPeerOperator(regionID, fromStoreID, toStoreID int64) error {
	return errors.New("Not supported")
}

func (pd *LocalClient) AddCreatePeerOperator(regionID, storeID int64) error {
	return errors.New("Not supported")
}

func (pd *LocalClient) GetAllTiFlashStores(zone, region string) ([]int64, map[int64]pdhttp.StoreInfo, error) {
	data, err := os.ReadFile(pd.StoresFile)
	if err != nil {
		return nil, nil, err
	}
	var stores pdhttp.StoresInfo
	err = json.Unmarshal(data, &stores)
	if err != nil {
		return nil, nil, errors.Annotate(err, fmt.Sprintf("get all TiFlash stores failed from %s", pd.StoresFile))
	}
	var storeIDs []int64
	storesMap := make(map[int64]pdhttp.StoreInfo)
	for _, store := range stores.Stores {
		var location_match = true // by default it is true because "zone"/"region" could be empty
		var engine_match = false  // by default it is false because tikv doesn't contains the "engine" label
		for _, label := range store.Store.Labels {
			if region != "" && label.Key == "region" && label.Value != region {
				continue
			}
			if zone != "" && label.Key == "zone" && label.Value != zone {
				continue
			}
			if label.Key == "engine" && label.Value == "tiflash" {
				// engine match, continue to check whether location is match
				engine_match = true
				continue
			}
		}
		if engine_match && location_match {
			storeIDs = append(storeIDs, store.Store.ID)
			storesMap[store.Store.ID] = store
		}
	}
	return storeIDs, storesMap, nil
}

func (pd *LocalClient) GetRegions() ([]pdhttp.RegionInfo, error) {
	panic("Do not support local")
}

func (pd *LocalClient) GetStoreRegionSetInGivenRange(storeID []int64, StartKey, EndKey []byte) ([]*client.TiFlashStoreRegionSet, error) {
	var allRegions []pdhttp.RegionInfo
	for _, f := range pd.RegionsFiles {
		data, err := os.ReadFile(f)
		if err != nil {
			return nil, errors.Annotate(err, fmt.Sprintf("failed to read from %s", f))
		}
		var regions pdhttp.RegionsInfo
		err = json.Unmarshal(data, &regions)
		if err != nil {
			return nil, errors.Annotate(err, fmt.Sprintf("failed to parse regions from %s", f))
		}
		if regions.Count == 0 {
			break
		}
		allRegions = append(allRegions, regions.Regions...)
	}

	storeIDSet := make(map[int64]struct{})
	for _, id := range storeID {
		storeIDSet[id] = struct{}{}
	}

	storeRegionSet := make(map[int64]map[int64]struct{})
	for _, region := range allRegions {
		for _, peer := range region.Peers {
			if _, ok := storeIDSet[peer.StoreID]; ok {
				if _, ok := storeRegionSet[peer.StoreID]; !ok {
					storeRegionSet[peer.StoreID] = make(map[int64]struct{})
				}
				storeRegionSet[peer.StoreID][region.ID] = struct{}{}
			}
		}
	}

	var result []*client.TiFlashStoreRegionSet
	for storeID, regionSet := range storeRegionSet {
		result = append(result, &client.TiFlashStoreRegionSet{ID: storeID, RegionIDSet: regionSet})
	}
	return result, nil
}

func (pd *LocalClient) GetTableKeyRange(tableID int64) ([]byte, []byte, error) {
	startKey := tidbcodec.NewTableStartAsKey(tableID)
	endKey := tidbcodec.NewTableEndAsKey(tableID)
	return startKey.GetBytes(), endKey.GetBytes(), nil
}
