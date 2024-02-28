package main

import (
	"fmt"

	"github.com/Lloyd-Pottiger/tiflash-replica-table-data-balancer/balancer"
	"github.com/spf13/cobra"
)

var cmd *cobra.Command

func init() {
	var (
		pdHost  string
		pdPort  int
		tableID int64
		SSLCA   string
		SSLCert string
		SSLKey  string
	)

	cmd = &cobra.Command{
		Use:   "balancer",
		Short: "A tool helps to balance the table data of TiFlash replicas between multiple TiFlash instances",
		Run: func(_ *cobra.Command, _ []string) {
			balancer.GlobalConfig = &balancer.Config{
				PDEndpoint: fmt.Sprintf("%s:%d", pdHost, pdPort),
				TableID:    tableID,
			}
			if len(SSLCA) != 0 && len(SSLCert) != 0 && len(SSLKey) != 0 {
				balancer.GlobalConfig.Security = &balancer.Security{
					SSLCA:   SSLCA,
					SSLCert: SSLCert,
					SSLKey:  SSLKey,
				}
			}
			balancer.InitPDClient()
			balancer.Schedule()
		},
	}

	cmd.PersistentFlags().BoolP("help", "h", false, "help for this command")
	cmd.PersistentFlags().StringVarP(&pdHost, "pd-host", "H", "127.0.0.1", "PD Host")
	cmd.PersistentFlags().IntVarP(&pdPort, "pd-port", "P", 2379, "PD Port")
	cmd.PersistentFlags().Int64VarP(&tableID, "table", "t", 0, "Table ID")
	cmd.PersistentFlags().StringVarP(&SSLCA, "ssl-ca", "", "", "SSL CA")
	cmd.PersistentFlags().StringVarP(&SSLCert, "ssl-cert", "", "", "SSL Cert")
	cmd.PersistentFlags().StringVarP(&SSLKey, "ssl-key", "", "", "SSL Key")

	cmd.MarkFlagRequired("table")
}

func main() {
	cmd.Execute()
}
