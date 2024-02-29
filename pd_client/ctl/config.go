package ctl

import (
	client "github.com/Lloyd-Pottiger/tiflash-replica-table-data-balancer/pd_client"
)

type CtlConfig struct {
	Path string
}

func (c *CtlConfig) GetClient() client.PDClient {
	return &PDCtl{
		Path: c.Path,
	}
}
