package cloud

import (
	"fmt"
	"log"
	"slices"
	"sync"
	"time"

	"google.golang.org/api/drive/v3"
)

func CleanOldCloudDumps(srv drive.Service, retention *time.Duration, wg *sync.WaitGroup) {
	// Get list of backups older than retention time

	backupFiles, err := srv.Files.List().PageSize(1000).Fields("files(id, name, createdTime, parents)").Do()
	if err != nil {
		log.Fatal(err)
	}

	for _, backupFile := range backupFiles.Files {
		fmt.Printf("Filename: %v, Created on: %v\n", backupFile.Name, backupFile.Parents)
		createdTime, err := time.Parse(time.RFC3339, backupFile.CreatedTime)
		if err != nil {
			log.Fatal(err)
		}
		backupAge := time.Since(createdTime).Hours()
		fmt.Printf("Backup Age in Hrs: %v\n", backupAge)

		if (backupAge > retention.Hours()) && (slices.Contains(backupFile.Parents, "0B6VheyHPSfoJRjMwbERBdW52ZTQ")) {
			// if backupFile.MimeType !=
			// err := srv.Files.Delete(backupFile.Id).Do()
			// if err != nil {
			// 	log.Fatal(err)
			// }
			fmt.Printf("File %v is deleted.\n", backupFile.Name)
		}

		// if err := srv.Files.EmptyTrash().Do(); err != nil {
		// 	log.Fatal(err)
		// }
	}

	fmt.Printf("No of Backup Files: %v\n", len(backupFiles.Files))

	// Delete those backups
	fmt.Printf("cleaned backups older than %v Hrs.\n", retention)

	defer wg.Done()
}
