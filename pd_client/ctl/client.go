package ctl

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strconv"

	client "github.com/Lloyd-Pottiger/tiflash-replica-table-data-balancer/pd_client"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	pdhttp "github.com/tikv/pd/client/http"
	"go.uber.org/zap"
)

type PDCtl struct {
	Path string
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
	_, err := execute(pd.Path, "operator", "add", "transfer-peer", strconv.FormatInt(regionID, 10), strconv.FormatInt(fromStoreID, 10), strconv.FormatInt(toStoreID, 10))
	return err
}

func (pd *PDCtl) GetAllTiFlashStores() ([]int64, error) {
	output, err := execute(pd.Path, "store", "--jq", `[.stores[] | select(.store.labels[]? | select(.key == "engine" and .value == "tiflash"))]`)
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
	output, err := execute(pd.Path, "region", "store", strconv.FormatInt(storeID, 10))
	if err != nil {
		return nil, err
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
	outputFile := fmt.Sprintf("rule-tiflash-table-%v-r.json", tableID)
	_, err := execute(pd.Path, "config", "placement-rules", "load", "--group", "tiflash", "--id", fmt.Sprintf("table-%v-r", tableID), "--out", outputFile)
	if err != nil {
		return nil, nil, err
	}
	data, err := os.ReadFile(outputFile)
	if err != nil {
		return nil, nil, err
	}
	var rules []pdhttp.Rule
	if err := json.Unmarshal(data, &rules); err != nil {
		return nil, nil, err
	}
	if len(rules) != 1 {
		return nil, nil, errors.New("invalid rule count")
	}
	os.Remove(outputFile)
	rule := &rules[0]
	startKey, endKey := client.Codec.EncodeRegionRange(rule.StartKey, rule.EndKey)
	return startKey, endKey, nil
}
