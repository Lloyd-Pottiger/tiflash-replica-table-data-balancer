package ctl

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os/exec"
	"strconv"

	tidbcodec "github.com/JaySon-Huang/tiflash-ctl/pkg/tidb"
	client "github.com/Lloyd-Pottiger/tiflash-replica-table-data-balancer/pd_client"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	pdhttp "github.com/tikv/pd/client/http"
	"go.uber.org/zap"
)

type PDCtl struct {
	Command string
	Args    []string
}

func execute(cmd string, args ...string) (string, error) {
	out, err := exec.Command(cmd, args...).Output()
	if err != nil {
		log.Error("Command execution failed", zap.String("command", cmd), zap.Strings("args", args), zap.Error(err))
		return "", err
	}

	log.Info("Command Successfully Executed: ", zap.String("command", cmd), zap.Strings("args", args))
	return string(out), nil
}

func (pd *PDCtl) AddTransferPeerOperator(regionID, fromStoreID, toStoreID int64) error {
	args := append(pd.Args, "operator", "add", "transfer-peer", strconv.FormatInt(regionID, 10), strconv.FormatInt(fromStoreID, 10), strconv.FormatInt(toStoreID, 10))
	_, err := execute(pd.Command, args...)
	return err
}

func (pd *PDCtl) GetAllTiFlashStores(zone, region string) ([]int64, error) {
	jqQuery := `select(.store.labels[]? | select(.key == "engine" and .value == "tiflash"))`
	if len(zone) > 0 && len(region) > 0 {
		jqQuery = fmt.Sprintf(`[.stores[] | %s | select(.store.labels[]? | (.key == "region" and .value == "%s"))] | select(.store.labels[]? | (.key == "zone" and .value == "%s"))]`, jqQuery, region, zone)
	} else if len(zone) > 0 {
		jqQuery = fmt.Sprintf(`[.stores[] | %s | select(.store.labels[]? | (.key == "zone" and .value == "%s"))]`, jqQuery, zone)
	} else if len(region) > 0 {
		jqQuery = fmt.Sprintf(`[.stores[] | %s | select(.store.labels[]? | (.key == "region" and .value == "%s"))]`, jqQuery, region)
	} else {
		jqQuery = fmt.Sprintf(`[.stores[] | %s]`, jqQuery)
	}
	args := append(pd.Args, "store", "--jq", jqQuery)
	output, err := execute(pd.Command, args...)
	if err != nil {
		return nil, err
	}
	var stores []pdhttp.StoreInfo
	err = json.Unmarshal([]byte(output), &stores)
	if err != nil {
		return nil, err
	}
	var storeIDs []int64
	for _, store := range stores {
		storeIDs = append(storeIDs, store.Store.ID)
	}
	return storeIDs, nil
}

func (pd *PDCtl) GetStoreRegionSetInGivenRange(storeID []int64, StartKey, EndKey []byte) ([]*client.TiFlashStoreRegionSet, error) {
	var allRegions []pdhttp.RegionInfo
	for {
		args := append(pd.Args, "region", "keys", hex.EncodeToString(StartKey), hex.EncodeToString(EndKey), strconv.FormatInt(client.DEFAULT_REGION_PER_BATCH, 10))
		output, err := execute(pd.Command, args...)
		if err != nil {
			return nil, errors.Annotate(err, "get regions by key range failed")
		}
		var regions pdhttp.RegionsInfo
		err = json.Unmarshal([]byte(output), &regions)
		if err != nil {
			return nil, errors.Annotate(err, "unmarshal regions failed")
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

func (pd *PDCtl) GetTableKeyRange(tableID int64) ([]byte, []byte, error) {
	startKey := tidbcodec.NewTableStartAsKey(tableID)
	endKey := tidbcodec.NewTableEndAsKey(tableID)
	return startKey.GetBytes(), endKey.GetBytes(), nil
}
