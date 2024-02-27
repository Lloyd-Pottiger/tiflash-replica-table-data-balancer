package balancer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/pingcap/errors"
)

func AddTransferPeerOperator(pdEndpoint string, regionID, fromStoreID, toStoreID int64) error {
	input := make(map[string]any)
	input["name"] = "transfer-peer"
	input["region_id"] = regionID
	input["from_store_id"] = fromStoreID
	input["to_store_id"] = toStoreID

	data, err := json.Marshal(input)
	if err != nil {
		return errors.Annotate(err, "marshal transfer peer operator failed")
	}
	return PostJSON(pdEndpoint, "/pd/api/v1/operators", data)
}

func GetAllTiFlashStores(pdEndpoint string) ([]int64, error) {
	if pdClient == nil {
		InitPDClient()
	}
	stores, err := pdClient.GetStores(context.Background())
	if err != nil {
		return nil, errors.Annotate(err, "get all TiFlash stores failed")
	}
	var storeIDs []int64
	for _, store := range stores.Stores {
		storeIDs = append(storeIDs, store.Store.ID)
	}
	return storeIDs, nil
}

func GetStoreRegionIDsInGivenRange(pdEndpoint string, storeID int64, startKey, endKey []byte) ([]int64, error) {
	if pdClient == nil {
		InitPDClient()
	}
	regions, err := pdClient.GetRegionsByStoreID(context.Background(), uint64(storeID))
	if err != nil {
		return nil, errors.Annotate(err, "get store regions failed")
	}
	var regionIDs []int64
	for _, region := range regions.Regions {
		if bytes.Compare([]byte(region.StartKey), startKey) >= 0 && (len(endKey) == 0 || bytes.Compare([]byte(region.EndKey), endKey) < 0) {
			regionIDs = append(regionIDs, region.ID)
		}
	}
	return regionIDs, nil
}

func GetTableKeyRange(pdEndpoint string, tableID int64) (startKey, endKey []byte, err error) {
	if pdClient == nil {
		InitPDClient()
	}
	rule, err := pdClient.GetPlacementRule(context.Background(), "tiflash", fmt.Sprintf("table-%v-r", tableID))
	if err != nil {
		return nil, nil, errors.Annotate(err, "get placement rule failed")
	}
	if rule.Count != 1 {
		// FIXME: support multiple replicas
		return nil, nil, errors.Errorf("Invalid placement rule count %v, now support 1 replica now", rule.Count)
	}
	return rule.StartKey, rule.EndKey, nil
}
