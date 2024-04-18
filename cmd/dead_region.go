package cmd

import (
	"fmt"

	balancer "github.com/Lloyd-Pottiger/tiflash-replica-table-data-balancer/balancer"
	"github.com/Lloyd-Pottiger/tiflash-replica-table-data-balancer/pd_client/http"
	"github.com/spf13/cobra"
)

func NewHttpRegionCmd() *cobra.Command {
	var (
		pdHost   string
		pdPort   int
		SSLCA    string
		SSLCert  string
		SSLKey   string
		Stores   []int
		Offline  bool
		dryRun   bool
		showOnly bool
	)

	cmd := &cobra.Command{
		Use:   "dead_region",
		Short: "Use PD HTTP API For Region Scheduling",
		Run: func(cmd *cobra.Command, args []string) {
			conf := http.HttpConfig{
				Endpoint: fmt.Sprintf("%s:%d", pdHost, pdPort),
				Security: &http.Security{
					SSLCA:   SSLCA,
					SSLCert: SSLCert,
					SSLKey:  SSLKey,
				},
			}
			client := conf.GetClient()
			var offlineStoreIds []int64
			for _, i := range Stores {
				offlineStoreIds = append(offlineStoreIds, int64(i))
			}
			if err := balancer.ScheduleRegion(client, offlineStoreIds, Offline, dryRun, showOnly); err != nil {
				cmd.PrintErrln(err)
			}
		},
	}

	cmd.PersistentFlags().BoolP("help", "h", false, "help for this command")
	cmd.Flags().StringVarP(&pdHost, "pd-host", "H", "127.0.0.1", "PD Host")
	cmd.Flags().IntVarP(&pdPort, "pd-port", "P", 2379, "PD Port")
	cmd.Flags().IntSliceVarP(&Stores, "stores", "s", []int{}, "Offline TiFlash store ids")
	cmd.Flags().BoolVarP(&Offline, "offline", "o", false, "Also set TiFlash store offline")
	cmd.Flags().StringVarP(&SSLCA, "ssl-ca", "", "", "SSL CA")
	cmd.Flags().StringVarP(&SSLCert, "ssl-cert", "", "", "SSL Cert")
	cmd.Flags().StringVarP(&SSLKey, "ssl-key", "", "", "SSL Key")
	cmd.Flags().BoolVarP(&dryRun, "dry-run", "", true, "Print the transfer peer operator without running")
	cmd.Flags().BoolVarP(&showOnly, "show-only", "", false, "")

	cmd.MarkFlagRequired("store")

	return cmd
}
