package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"google.golang.org/api/drive/v3"
	"google.golang.org/api/option"

	"dxdbbackup/cloud"
	"dxdbbackup/local"
)

var wg = sync.WaitGroup{}

func main() {
	fmt.Println("DX DB Backup Script")

	srv, err := drive.NewService(
		context.Background(),
		option.WithCredentialsFile("balmy-moonlight-196910-348c8ecd1dca.json"),
	)
	if err != nil {
		log.Fatalf("Unable to retrieve Drive client: %v", err)
	}

	// 1. List DBs for backups excluding default DBs
	databases := local.GetDbList()

	// Steps 2 & 3
	wg.Add(len(databases) + 2)
	for _, database := range databases {
		go func(database string) {
			// 2. Take DB dump of mysql database
			zipPath := local.TakeDump(database)
			// 3. Upload the DB dumps to Google Drive
			cloud.UploadToCloud(*srv, zipPath, &wg)
		}(database)
	}

	// 5. Cleanup older dumps
	retentionTime := "15m"
	retention, err := time.ParseDuration(retentionTime)
	if err != nil {
		log.Fatal(err)
	}
	go local.CleanOldLocalDumps(&retention, &wg)
	go cloud.CleanOldCloudDumps(*srv, &retention, &wg)

	wg.Wait()
}
