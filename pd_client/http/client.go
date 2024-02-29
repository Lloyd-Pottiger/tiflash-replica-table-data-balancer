package http

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"

	client "github.com/Lloyd-Pottiger/tiflash-replica-table-data-balancer/pd_client"
	"github.com/pingcap/errors"
	pdhttp "github.com/tikv/pd/client/http"
)

type PDHttp struct {
	Endpoint      string
	Client        pdhttp.Client
	rawHttpClient *http.Client
	schema        string
}

func (pd *PDHttp) AddTransferPeerOperator(regionID, fromStoreID, toStoreID int64) error {
	// TODO: Use PD HTTP SDK when it is available
	input := make(map[string]any)
	input["name"] = "transfer-peer"
	input["region_id"] = regionID
	input["from_store_id"] = fromStoreID
	input["to_store_id"] = toStoreID

	data, err := json.Marshal(input)
	if err != nil {
		return errors.Annotate(err, "marshal transfer peer operator failed")
	}
	return postJSON(pd.rawHttpClient, pd.schema, pd.Endpoint, "/pd/api/v1/operators", data)
}

func (pd *PDHttp) GetAllTiFlashStores() ([]int64, error) {
	stores, err := pd.Client.GetStores(context.Background())
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

func (pd *PDHttp) GetStoreRegionIDsInGivenRange(storeID int64, startKey, endKey []byte) ([]int64, error) {
	regions, err := pd.Client.GetRegionsByStoreID(context.Background(), uint64(storeID))
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

func (pd *PDHttp) GetTableKeyRange(tableID int64) ([]byte, []byte, error) {
	rule, err := pd.Client.GetPlacementRule(context.Background(), "tiflash", fmt.Sprintf("table-%v-r", tableID))
	if err != nil {
		return nil, nil, errors.Annotate(err, "get placement rule failed")
	}
	startKey, endKey := client.Codec.EncodeRegionRange(rule.StartKey, rule.EndKey)
	return startKey, endKey, nil
}
