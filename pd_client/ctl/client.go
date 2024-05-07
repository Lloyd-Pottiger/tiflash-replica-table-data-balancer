package ctl

import (
	"encoding/hex"
	"encoding/json"
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

func (pd *PDCtl) AddCreatePeerOperator(regionID, storeID int64) error {
	return errors.New("Do not support pd-ctl")
}

func (pd *PDCtl) GetAllTiFlashStores(zone, region string) ([]int64, map[int64]pdhttp.StoreInfo, error) {
	// Note: do not use `jq` because the binary is not always available
	args := append(pd.Args, "store")
	output, err := execute(pd.Command, args...)
	if err != nil {
		return nil, nil, errors.Annotate(err, "get all TiFlash stores failed")
	}
	var stores pdhttp.StoresInfo
	err = json.Unmarshal([]byte(output), &stores)
	if err != nil {
		return nil, nil, errors.Annotate(err, "get all TiFlash stores failed")
	}
	return client.GetAllTiFlashStores(stores, zone, region)
}

func (pd *PDCtl) GetRegions() ([]pdhttp.RegionInfo, error) {
	return nil, errors.New("Not supported")
}

func (pd *PDCtl) DeleteStore(storeID int64) error {
	return errors.New("Not supported")
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
		// no more regions left
		if regions.Count == 0 {
			break
		}

		allRegions = append(allRegions, regions.Regions...)
		endRegion := regions.Regions[len(regions.Regions)-1]
		if len(endRegion.EndKey) == 0 {
			break
		}
		// check whether there are more regions
		endKey, err := hex.DecodeString(endRegion.EndKey)
		if err != nil {
			return nil, errors.Annotate(err, "decode end key failed")
		}
		StartKey = endKey
	}

	return client.GetStoreRegionSetByStoreID(allRegions, storeID)
}

func (pd *PDCtl) GetTableKeyRange(tableID int64) ([]byte, []byte, error) {
	startKey := tidbcodec.NewTableStartAsKey(tableID)
	endKey := tidbcodec.NewTableEndAsKey(tableID)
	return startKey.GetBytes(), endKey.GetBytes(), nil
}
