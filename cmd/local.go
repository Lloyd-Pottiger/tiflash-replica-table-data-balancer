package cmd

import (
	"os"

	"github.com/Lloyd-Pottiger/tiflash-replica-table-data-balancer/balancer"
	"github.com/Lloyd-Pottiger/tiflash-replica-table-data-balancer/pd_client/local"
	"github.com/spf13/cobra"
)

func NewLocalCmd() *cobra.Command {
	var (
		StoresFile   string
		RegionsFiles []string
		tableID      int64
		zone         string
		region       string
		dryRun       bool
		showOnly     bool
	)
	cmd := &cobra.Command{
		Use:   "local",
		Short: "Use local json file as input",
		PreRun: func(cmd *cobra.Command, args []string) {
			if len(args) == 0 {
				cmd.Help()
				os.Exit(0)
			}
		},
		Run: func(cmd *cobra.Command, args []string) {
			client := &local.LocalClient{
				StoresFile:   StoresFile,
				RegionsFiles: RegionsFiles,
			}
			if err := balancer.Schedule(client, tableID, zone, region, dryRun, showOnly); err != nil {
				cmd.PrintErrln(err)
			}
		},
	}

	cmd.PersistentFlags().BoolP("help", "h", false, "help for this command")
	cmd.Flags().StringVarP(&StoresFile, "stores", "", "", "The store info in JSON format")
	cmd.Flags().StringArrayVarP(&RegionsFiles, "regions", "", []string{}, "The regions info in JSON format")
	cmd.Flags().Int64VarP(&tableID, "table", "t", 0, "Table ID")
	cmd.Flags().StringVarP(&zone, "zone", "z", "", "Zone Name")
	cmd.Flags().StringVarP(&region, "region", "r", "", "Region Name")
	cmd.Flags().BoolVarP(&dryRun, "dry-run", "", true, "Print the transfer peer operator without running")
	cmd.Flags().BoolVarP(&showOnly, "show-only", "", false, "Only show the region distribution")

	cmd.MarkFlagRequired("stores")
	cmd.MarkFlagRequired("regions")

	return cmd
}
