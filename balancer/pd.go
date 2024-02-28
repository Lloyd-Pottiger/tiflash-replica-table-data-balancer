package balancer

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/pingcap/errors"
	"github.com/tikv/client-go/v2/tikv"
)

var Codec = tikv.NewCodecV1(tikv.ModeTxn)

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
		for _, label := range store.Store.Labels {
			if label.Key == "engine" && label.Value == "tiflash" {
				storeIDs = append(storeIDs, store.Store.ID)
				break
			}
		}
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
		regionStartKey, err := hex.DecodeString(region.StartKey)
		if err != nil {
			return nil, errors.Annotate(err, "decode region start key failed")
		}
		regionEndKey, err := hex.DecodeString(region.EndKey)
		if err != nil {
			return nil, errors.Annotate(err, "decode region end key failed")
		}
		if bytes.Compare(regionStartKey, startKey) >= 0 && bytes.Compare(regionEndKey, endKey) <= 0 {
			regionIDs = append(regionIDs, region.ID)
		}
	}
	return regionIDs, nil
}

func GetTableKeyRange(pdEndpoint string, tableID int64) ([]byte, []byte, error) {
	if pdClient == nil {
		InitPDClient()
	}
	rule, err := pdClient.GetPlacementRule(context.Background(), "tiflash", fmt.Sprintf("table-%v-r", tableID))
	if err != nil {
		return nil, nil, errors.Annotate(err, "get placement rule failed")
	}
	startKey, endKey := Codec.EncodeRegionRange(rule.StartKey, rule.EndKey)
	return startKey, endKey, nil
}
