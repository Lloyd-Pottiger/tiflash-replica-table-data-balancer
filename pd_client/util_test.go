package pdclient_test

import (
	"testing"

	pdclient "github.com/Lloyd-Pottiger/tiflash-replica-table-data-balancer/pd_client"
	pdhttp "github.com/tikv/pd/client/http"
)

func TestGetAllTiFlashStores(t *testing.T) {
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
	if err != nil {
		t.Errorf("error happen, %s", err)
	}
	if len(ids) != 1 || ids[0] != 101 {
		t.Errorf("ids not as expected, %v", ids)
	}
}
