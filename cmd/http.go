package cmd

import (
	"fmt"

	balancer "github.com/Lloyd-Pottiger/tiflash-replica-table-data-balancer/balancer"
	"github.com/Lloyd-Pottiger/tiflash-replica-table-data-balancer/pd_client/http"
	"github.com/spf13/cobra"
)

func NewHttpCmd() *cobra.Command {
	var (
		pdHost  string
		pdPort  int
		SSLCA   string
		SSLCert string
		SSLKey  string
		tableID int64
	)

	cmd := &cobra.Command{
		Use:   "http",
		Short: "Use PD HTTP API",
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
			if err := balancer.Schedule(client, tableID); err != nil {
				cmd.PrintErrln(err)
			}
		},
	}

	cmd.PersistentFlags().BoolP("help", "h", false, "help for this command")
	cmd.PersistentFlags().StringVarP(&pdHost, "pd-host", "H", "127.0.0.1", "PD Host")
	cmd.PersistentFlags().IntVarP(&pdPort, "pd-port", "P", 2379, "PD Port")
	cmd.PersistentFlags().StringVarP(&SSLCA, "ssl-ca", "", "", "SSL CA")
	cmd.PersistentFlags().StringVarP(&SSLCert, "ssl-cert", "", "", "SSL Cert")
	cmd.PersistentFlags().StringVarP(&SSLKey, "ssl-key", "", "", "SSL Key")
	cmd.PersistentFlags().Int64VarP(&tableID, "table", "t", 0, "Table ID")

	cmd.MarkFlagRequired("table")

	return cmd
}
