package main

import (
	"github.com/Lloyd-Pottiger/tiflash-replica-table-data-balancer/cmd"
	"github.com/pingcap/errors"
	"github.com/spf13/cobra"
)

var rootCmd *cobra.Command

func init() {
	rootCmd = &cobra.Command{
		Use:                "balancer",
		Short:              "A tool helps to balance the table data of TiFlash replicas between multiple TiFlash instances",
		DisableFlagParsing: true,
		SilenceUsage:       true,
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				return cmd.Help()
			}
			switch args[0] {
			case "--help", "-h":
				return cmd.Help()
			default:
				return errors.Errorf("unknown flag: %s\nRun `balancer --help` for usage.", args[0])
			}
		},
	}
	rootCmd.AddCommand(
		cmd.NewHttpCmd(),
		cmd.NewCtlCmd(),
	)
}

func main() {
	rootCmd.Execute()
}
