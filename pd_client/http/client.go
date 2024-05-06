package http

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"

	tidbcodec "github.com/JaySon-Huang/tiflash-ctl/pkg/tidb"
	client "github.com/Lloyd-Pottiger/tiflash-replica-table-data-balancer/pd_client"
	pdclient "github.com/Lloyd-Pottiger/tiflash-replica-table-data-balancer/pd_client"
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

func (pd *PDHttp) AddCreatePeerOperator(regionID, storeID int64) error {
	// TODO: Use PD HTTP SDK when it is available
	input := make(map[string]any)
	input["name"] = "transfer-peer"
	input["region_id"] = regionID
	input["store_id"] = storeID

	data, err := json.Marshal(input)
	if err != nil {
		return errors.Annotate(err, "marshal transfer peer operator failed")
	}
	return postJSON(pd.rawHttpClient, pd.schema, pd.Endpoint, "/pd/api/v1/operators", data)
}

func (pd *PDHttp) DeleteStore(storeID int64) error {
	return deleteHTTP(pd.rawHttpClient, pd.schema, pd.Endpoint, fmt.Sprintf("/pd/api/v1/store/%v", storeID))
}

func (pd *PDHttp) GetAllTiFlashStores(zone, region string) ([]int64, map[int64]pdhttp.StoreInfo, error) {
	stores, err := pd.Client.GetStores(context.Background())
	if err != nil {
		return nil, nil, errors.Annotate(err, "get all TiFlash stores failed")
	}
	return pdclient.GetAllTiFlashStores(*stores, zone, region)
}

func (pd *PDHttp) GetRegions() ([]pdhttp.RegionInfo, error) {
	result, err := pd.Client.GetRegions(context.Background())
	if err != nil {
		return nil, errors.Annotate(err, "get all TiFlash regions failed")
	}
	return result.Regions, nil
}

func (pd *PDHttp) GetStoreRegionSetInGivenRange(storeID []int64, StartKey, EndKey []byte) ([]*client.TiFlashStoreRegionSet, error) {
	var allRegions []pdhttp.RegionInfo
	for {
		keyRange := pdhttp.NewKeyRange(StartKey, EndKey)
		regions, err := pd.Client.GetRegionsByKeyRange(context.Background(), keyRange, client.DEFAULT_REGION_PER_BATCH)
		if err != nil {
			return nil, errors.Annotate(err, "get regions by key range failed")
		}
		if regions.Count == 0 {
			break
		}
		allRegions = append(allRegions, regions.Regions...)

		endRegion := regions.Regions[len(regions.Regions)-1]
		if len(endRegion.EndKey) == 0 {
			break
		}
		endKey, err := hex.DecodeString(endRegion.EndKey)
		if err != nil {
			return nil, errors.Annotate(err, "decode end key failed")
		}
		StartKey = endKey
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

func (pd *PDHttp) GetTableKeyRange(tableID int64) ([]byte, []byte, error) {
	startKey := tidbcodec.NewTableStartAsKey(tableID)
	endKey := tidbcodec.NewTableEndAsKey(tableID)
	return startKey.GetBytes(), endKey.GetBytes(), nil
}
