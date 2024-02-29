package cmd

import (
	"fmt"
	"strings"

	balancer "github.com/Lloyd-Pottiger/tiflash-replica-table-data-balancer/balancer"
	"github.com/Lloyd-Pottiger/tiflash-replica-table-data-balancer/pd_client/ctl"
	"github.com/spf13/cobra"
)

func NewCtlCmd() *cobra.Command {
	var (
		ctlPath string
		pdHost  string
		pdPort  int
		SSLCA   string
		SSLCert string
		SSLKey  string
		tableID int64
		zone    string
		region  string
	)

	cmd := &cobra.Command{
		Use:   "ctl",
		Short: "Use pd-ctl",
		Run: func(cmd *cobra.Command, _ []string) {
			var conf *ctl.CtlConfig
			if !strings.HasSuffix(ctlPath, ".sh") {
				conf = &ctl.CtlConfig{
					Command: ctlPath,
				}
			} else {
				conf = &ctl.CtlConfig{
					Command: "/bin/sh",
					Args:    []string{ctlPath},
				}
			}
			if pdHost != "" && pdPort != 0 {
				conf.Args = append(conf.Args, "--pd")
				if len(SSLCA) > 0 && len(SSLCert) > 0 && len(SSLKey) > 0 {
					conf.Args = append(conf.Args, fmt.Sprintf("https://%s:%d", pdHost, pdPort))
				} else {
					conf.Args = append(conf.Args, fmt.Sprintf("http://%s:%d", pdHost, pdPort))
				}
			}
			if SSLCA != "" {
				conf.Args = append(conf.Args, "--cacert")
				conf.Args = append(conf.Args, SSLCA)
			}
			if SSLCert != "" {
				conf.Args = append(conf.Args, "--cert")
				conf.Args = append(conf.Args, SSLCert)
			}
			if SSLKey != "" {
				conf.Args = append(conf.Args, "--key")
				conf.Args = append(conf.Args, SSLKey)
			}
			client := conf.GetClient()
			if err := balancer.Schedule(client, tableID, zone, region); err != nil {
				cmd.PrintErrln(err)
			}
		},
	}

	cmd.PersistentFlags().BoolP("help", "", false, "help for this command")
	cmd.Flags().StringVarP(&ctlPath, "ctl-path", "p", "pd-ctl", "The path of pd-ctl")
	cmd.Flags().Int64VarP(&tableID, "table", "t", 0, "Table ID")
	cmd.Flags().StringVarP(&pdHost, "pd-host", "H", "127.0.0.1", "PD Host")
	cmd.Flags().IntVarP(&pdPort, "pd-port", "P", 2379, "PD Port")
	cmd.Flags().StringVarP(&SSLCA, "ssl-ca", "", "", "SSL CA")
	cmd.Flags().StringVarP(&SSLCert, "ssl-cert", "", "", "SSL Cert")
	cmd.Flags().StringVarP(&SSLKey, "ssl-key", "", "", "SSL Key")
	cmd.Flags().StringVarP(&zone, "zone", "z", "", "Zone Name")
	cmd.Flags().StringVarP(&region, "region", "r", "", "Region Name")

	cmd.MarkFlagRequired("table")

	return cmd
}
