package pdclient

import pdhttp "github.com/tikv/pd/client/http"

func GetAllTiFlashStores(stores pdhttp.StoresInfo, zone, region string) ([]int64, map[int64]pdhttp.StoreInfo, error) {
	var storeIDs []int64
	storesMap := make(map[int64]pdhttp.StoreInfo)
	for _, store := range stores.Stores {
		var location_match = true // by default it is true because "zone"/"region" could be empty
		var engine_match = false  // by default it is false because tikv doesn't contains the "engine" label
		for _, label := range store.Store.Labels {
			if region != "" && label.Key == "region" && label.Value != region {
				location_match = false
				continue
			}
			if zone != "" && label.Key == "zone" && label.Value != zone {
				location_match = false
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
