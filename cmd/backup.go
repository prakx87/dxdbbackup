/*
Copyright © 2024 Prakyath Raj <EMAIL ADDRESS>
*/
package cmd

import (
	"context"
	"dxdbbackup/cloud"
	"dxdbbackup/local"
	"log"
	"sync"

	"github.com/spf13/cobra"
	"google.golang.org/api/drive/v3"
	"google.golang.org/api/option"
)

var wg = sync.WaitGroup{}

// backupCmd represents the backup command
var backupCmd = &cobra.Command{
	Use:   "backup",
	Short: "Backup is a palette that contains backup based commands",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Help()
	},
}

func init() {

	var dbCreds local.Dbcreds

	backupCmd.Flags().StringVarP(&dbCreds.Db, "database", "d", "mysql", "DB Name")
	backupCmd.Flags().StringVarP(&dbCreds.Host, "host", "H", "localhost", "DB Hostname")
	backupCmd.Flags().StringVarP(&dbCreds.User, "user", "u", "", "DB User")
	backupCmd.Flags().StringVarP(&dbCreds.Password, "password", "p", "", "DB Password")
	backupCmd.Flags().Uint16VarP(&dbCreds.Port, "port", "P", 3306, "DB Port")

	srv, err := drive.NewService(
		context.Background(),
		option.WithCredentialsFile("balmy-moonlight-196910-348c8ecd1dca.json"),
	)
	if err != nil {
		log.Fatalf("Unable to retrieve Drive client: %v", err)
	}

	// 1. List DBs for backups excluding default DBs
	databases := local.GetDbList(&dbCreds)

	// Steps 2 & 3
	wg.Add(len(databases) + 2)
	for _, database := range databases {
		go func(database string) {
			// 2. Take DB dump of mysql database
			zipPath := local.TakeDump(database)
			// 3. Upload the DB dumps to Google Drive
			cloud.UploadToCloud(*srv, zipPath)
			wg.Done()
		}(database)
	}

	wg.Wait()
}
