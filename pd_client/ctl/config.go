package ctl

import (
	client "github.com/Lloyd-Pottiger/tiflash-replica-table-data-balancer/pd_client"
)

type CtlConfig struct {
	Command string
	Args    []string
}

func (c *CtlConfig) GetClient() client.PDClient {
	return &PDCtl{
		Command: c.Command,
		Args:    c.Args,
	}
}
