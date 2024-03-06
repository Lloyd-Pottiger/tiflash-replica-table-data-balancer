package ctl

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os/exec"
	"strconv"

	tidbcodec "github.com/JaySon-Huang/tiflash-ctl/pkg/tidb"
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

func (pd *PDCtl) GetStoreRegionIDsInGivenRange(storeID int64, startKey, endKey []byte) ([]int64, error) {
	args := append(pd.Args, "region", "store", strconv.FormatInt(storeID, 10))
	output, err := execute(pd.Command, args...)
	if err != nil {
		return nil, err
	}
	if len(output) == 0 || output == "[]\n\n" {
		return nil, nil
	}
	var regions pdhttp.RegionsInfo
	err = json.Unmarshal([]byte(output), &regions)
	if err != nil {
		return nil, err
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

func (pd *PDCtl) GetTableKeyRange(tableID int64) ([]byte, []byte, error) {
	startKey := tidbcodec.NewTableStartAsKey(tableID)
	endKey := tidbcodec.NewTableEndAsKey(tableID)
	return startKey.GetBytes(), endKey.GetBytes(), nil
}
