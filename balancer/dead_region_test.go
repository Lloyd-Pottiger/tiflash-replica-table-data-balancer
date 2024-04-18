package balancer_test

import (
	"testing"

	"github.com/Lloyd-Pottiger/tiflash-replica-table-data-balancer/balancer"
	pdhttp "github.com/tikv/pd/client/http"
)

func TestPick(t *testing.T) {
	store1 := pdhttp.StoreInfo{}
	store1.Store.ID = 1
	store1.Store.Labels = append(store1.Store.Labels, pdhttp.StoreLabel{
		"zone", "a"})
	store2 := pdhttp.StoreInfo{}
	store2.Store.ID = 2
	store2.Store.Labels = append(store1.Store.Labels, pdhttp.StoreLabel{
		"region", "b"})
	store3 := pdhttp.StoreInfo{}
	store3.Store.ID = 3
	store3.Store.Labels = append(store1.Store.Labels, pdhttp.StoreLabel{
		"zone", "a"})
	store3.Store.Labels = append(store1.Store.Labels, pdhttp.StoreLabel{
		"region", "c"})
	store4 := pdhttp.StoreInfo{}
	store4.Store.ID = 4
	store4.Store.Labels = append(store1.Store.Labels, pdhttp.StoreLabel{
		"zone", "a"})
	store4.Store.Labels = append(store1.Store.Labels, pdhttp.StoreLabel{
		"region", "c"})

	stores := make(map[int64]pdhttp.StoreInfo)
	stores[1] = store1
	stores[2] = store2
	stores[3] = store3
	stores[4] = store4

	if balancer.LocationLabelMatch(stores, &store1, []int64{2}) {
		t.Errorf("error")
	}
	if balancer.LocationLabelMatch(stores, &store1, []int64{3}) {
		t.Errorf("error")
	}
	if balancer.LocationLabelMatch(stores, &store1, []int64{4}) {
		t.Errorf("error")
	}
	if !balancer.LocationLabelMatch(stores, &store3, []int64{4}) {
		t.Errorf("error")
	}
}
