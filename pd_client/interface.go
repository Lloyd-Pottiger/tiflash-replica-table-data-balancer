package pdclient

import "github.com/tikv/client-go/v2/tikv"

var Codec = tikv.NewCodecV1(tikv.ModeTxn)

type PDClient interface {
	AddTransferPeerOperator(regionID, fromStoreID, toStoreID int64) error
	GetAllTiFlashStores() ([]int64, error)
	GetStoreRegionIDsInGivenRange(storeID int64, startKey, endKey []byte) ([]int64, error)
	GetTableKeyRange(tableID int64) ([]byte, []byte, error)
}

type ClientConfig interface {
	GetClient() PDClient
}
