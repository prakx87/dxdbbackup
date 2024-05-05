package local

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"
)

func CleanOldLocalDumps(retention *time.Duration, wg *sync.WaitGroup) {
	// Get list of backups older than retention time
	fmt.Printf("Retention Time: %v\n", retention)

	backupList, err := os.ReadDir("backups")
	if err != nil {
		log.Fatal(err)
	}

	// Delete those backups
	for _, backup := range backupList {
		backupInfo, err := backup.Info()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Name: %v, Info: %v\n", backup.Name(), backupInfo.ModTime())

		backupAge := time.Since(backupInfo.ModTime()).Hours()
		fmt.Printf("Backup Age in Hrs: %v\n", backupAge)
		if backupAge > retention.Hours() {
			err := os.Remove("backups/" + backup.Name())
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("File %v is deleted.\n", backup.Name())
		}
	}
	fmt.Printf("cleaned backups older than %v Hrs.\n", retention)

	defer wg.Done()
}
