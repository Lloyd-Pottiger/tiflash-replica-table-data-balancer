package pdclient

import (
	pdhttp "github.com/tikv/pd/client/http"
)

const (
	DEFAULT_REGION_PER_BATCH = 128
)

type PDClient interface {
	AddTransferPeerOperator(regionID, fromStoreID, toStoreID int64) error
	AddCreatePeerOperator(regionID, storeID int64) error
	GetAllTiFlashStores(zone, region string) ([]int64, map[int64]pdhttp.StoreInfo, error)
	GetRegions() ([]pdhttp.RegionInfo, error)
	GetStoreRegionSetInGivenRange(storeID []int64, startKey, endKey []byte) ([]*TiFlashStoreRegionSet, error)
	GetTableKeyRange(tableID int64) ([]byte, []byte, error)
}

type ClientConfig interface {
	GetClient() PDClient
}
