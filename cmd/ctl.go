package cmd

import (
	balancer "github.com/Lloyd-Pottiger/tiflash-replica-table-data-balancer/balancer"
	"github.com/Lloyd-Pottiger/tiflash-replica-table-data-balancer/pd_client/ctl"
	"github.com/spf13/cobra"
)

func NewCtlCmd() *cobra.Command {
	var (
		ctlPath  string
		SSLCA    string
		SSLCert  string
		SSLKey   string
		tableID  int64
		zone     string
		region   string
		dryRun   bool
		showOnly bool
	)

	cmd := &cobra.Command{
		Use:   "ctl",
		Short: "Use pd-ctl",
		Run: func(cmd *cobra.Command, _ []string) {
			var conf *ctl.CtlConfig
			// The script of `ctlPath` should be:
			// - Executable
			// - Do not need to specify the pd host/port
			conf = &ctl.CtlConfig{
				Command: ctlPath,
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
			if err := balancer.Schedule(client, tableID, zone, region, dryRun, showOnly); err != nil {
				cmd.PrintErrln(err)
			}
		},
	}

	cmd.PersistentFlags().BoolP("help", "", false, "help for this command")
	cmd.Flags().StringVarP(&ctlPath, "ctl-path", "p", "pd-ctl", "The path of pd-ctl")
	cmd.Flags().Int64VarP(&tableID, "table", "t", 0, "Table ID")
	cmd.Flags().StringVarP(&SSLCA, "ssl-ca", "", "", "SSL CA")
	cmd.Flags().StringVarP(&SSLCert, "ssl-cert", "", "", "SSL Cert")
	cmd.Flags().StringVarP(&SSLKey, "ssl-key", "", "", "SSL Key")
	cmd.Flags().StringVarP(&zone, "zone", "z", "", "Zone Name")
	cmd.Flags().StringVarP(&region, "region", "r", "", "Region Name")
	cmd.Flags().BoolVarP(&dryRun, "dry-run", "", true, "Print the transfer peer operator without running")
	cmd.Flags().BoolVarP(&showOnly, "show-only", "", false, "Only show the region distribution")

	cmd.MarkFlagRequired("table")

	return cmd
}
