package pdclient_test

import (
	"testing"

	pdclient "github.com/Lloyd-Pottiger/tiflash-replica-table-data-balancer/pd_client"
	"github.com/stretchr/testify/assert"
	pdhttp "github.com/tikv/pd/client/http"
)

func TestGetAllTiFlashStores(t *testing.T) {
	assert := assert.New(t)
	store_arr := []pdhttp.StoreInfo{
		{
			Store: pdhttp.MetaStore{
				ID:     1,
				Labels: []pdhttp.StoreLabel{{Key: "zone", Value: "a"}},
			},
		},
		{
			Store: pdhttp.MetaStore{
				ID:     2,
				Labels: []pdhttp.StoreLabel{{Key: "zone", Value: "b"}},
			},
		},
		{
			Store: pdhttp.MetaStore{
				ID:     3,
				Labels: []pdhttp.StoreLabel{{Key: "zone", Value: "c"}},
			},
		},
		{
			Store: pdhttp.MetaStore{
				ID:     101,
				Labels: []pdhttp.StoreLabel{{Key: "zone", Value: "a"}, {Key: "engine", Value: "tiflash"}},
			},
		},
		{
			Store: pdhttp.MetaStore{
				ID:     102,
				Labels: []pdhttp.StoreLabel{{Key: "zone", Value: "b"}, {Key: "engine", Value: "tiflash"}},
			},
		},
		{
			Store: pdhttp.MetaStore{
				ID:     103,
				Labels: []pdhttp.StoreLabel{{Key: "zone", Value: "c"}, {Key: "engine", Value: "tiflash"}},
			},
		},
	}
	stores := pdhttp.StoresInfo{Count: len(store_arr), Stores: store_arr}

	ids, _, err := pdclient.GetAllTiFlashStores(stores, "a", "")
	assert.Nil(err, nil)
	assert.Equal(ids, []int64{101})

	ids, _, err = pdclient.GetAllTiFlashStores(stores, "b", "")
	assert.Nil(err, nil)
	assert.Equal(ids, []int64{102})

	ids, _, err = pdclient.GetAllTiFlashStores(stores, "c", "")
	assert.Nil(err, nil)
	assert.Equal(ids, []int64{103})

	ids, _, err = pdclient.GetAllTiFlashStores(stores, "", "")
	assert.Nil(err, nil)
	assert.Equal(ids, []int64{101, 102, 103})
}

func TestGetStoreRegionSetByStoreID(t *testing.T) {
	assert := assert.New(t)
	allRegions := []pdhttp.RegionInfo{
		{
			ID:    1,
			Peers: []pdhttp.RegionPeer{{StoreID: 101}, {StoreID: 102}},
		},
	}

	storeID := []int64{101, 102, 103, 104, 105}
	regionByStore, err := pdclient.GetStoreRegionSetByStoreID(allRegions, storeID)
	assert.Nil(err)
	// the output size should be the same as `storeID` size
	assert.Equal(len(storeID), len(regionByStore))

	storeID = []int64{101, 102}
	regionByStore, err = pdclient.GetStoreRegionSetByStoreID(allRegions, storeID)
	assert.Nil(err)
	// the output size should be the same as `storeID` size
	assert.Equal(len(storeID), len(regionByStore))
}
