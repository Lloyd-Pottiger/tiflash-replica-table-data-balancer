package cmd

import (
	balancer "github.com/Lloyd-Pottiger/tiflash-replica-table-data-balancer/balancer"
	"github.com/Lloyd-Pottiger/tiflash-replica-table-data-balancer/pd_client/ctl"
	"github.com/spf13/cobra"
)

func NewCtlCmd() *cobra.Command {
	var (
		ctlPath string
		tableID int64
	)

	cmd := &cobra.Command{
		Use:   "ctl",
		Short: "Use pd-ctl",
		Run: func(cmd *cobra.Command, _ []string) {
			conf := &ctl.CtlConfig{
				Path: ctlPath,
			}
			client := conf.GetClient()
			if err := balancer.Schedule(client, tableID); err != nil {
				cmd.PrintErrln(err)
			}
		},
	}

	cmd.PersistentFlags().BoolP("help", "", false, "help for this command")
	cmd.Flags().StringVarP(&ctlPath, "ctl-path", "p", "pd-ctl", "The path of pd-ctl")
	cmd.PersistentFlags().Int64VarP(&tableID, "table", "t", 0, "Table ID")

	cmd.MarkFlagRequired("table")

	return cmd
}
