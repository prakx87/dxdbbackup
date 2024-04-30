package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"google.golang.org/api/drive/v3"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

var wg = sync.WaitGroup{}

func main() {
	fmt.Println("DX DB Backup Script")

	// 1. List DBs for backups excluding default DBs
	databases := getDbList()

	// Steps 2 & 3
	wg.Add(len(databases))
	for _, database := range databases {
		go func(database string) {
			// 2. Take DB dump of mysql database
			zipPath := takeDump(database)
			// 3. Upload the DB dumps to Google Drive
			uploadToCloud(zipPath)
		}(database)
	}

	// 5. Cleanup older dumps
	retentionTime := "15m"
	retention, err := time.ParseDuration(retentionTime)
	if err != nil {
		log.Fatal(err)
	}
	cleanOldLocalDumps(&retention)
	cleanOldCloudDumps(&retention)

	// wg.Wait()
}

func getDbList() []string {
	// mysql -h 127.0.0.1 -u root -pmy-secret-pw -e 'show databases;'
	cmd := exec.Command("/usr/bin/mysql", "-h", "127.0.0.1", "-uroot", "-pmy-secret-pw")

	query := strings.NewReader("show databases;")
	cmd.Stdin = query

	var out, stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr

	err := cmd.Run()
	if err != nil {
		log.Fatal(err)
	}

	// Get list of DBs from output, convert to list and ignore the 'Database' header at the top of list
	allDbs := strings.Fields(out.String())[1:]
	fmt.Printf("List of All Databases: %v\n", allDbs)
	// Create a list of Default DBs which needs to be ignored
	defaultDbs := []string{"information_schema", "mysql", "performance_schema", "sys"}
	databases := []string{}

	for _, db := range allDbs {
		if !slices.Contains(defaultDbs, db) {
			databases = append(databases, db)
		}
	}

	fmt.Printf("List of Databases to take backup: %v\n", databases)
	return databases
}

func takeDump(database string) string {
	//  mysqldump -h 127.0.0.1 -u root -pmy-secret-pw semaphore > dump.sql
	dumpCmd := exec.Command("/usr/bin/mysqldump", "-h", "127.0.0.1", "-uroot", "-pmy-secret-pw", database)
	// Create stdout io pipe in order to write output to file
	stdOut, err := dumpCmd.StdoutPipe()
	if err != nil {
		log.Fatal(err)
	}

	// Create dump file
	backupFolder := filepath.Join(".", "backups")
	if _, err := os.Stat(backupFolder); os.IsNotExist(err) {
		err := os.Mkdir(backupFolder, 0755)
		if err != nil {
			log.Fatal(err)
		}
	}

	zipName := fmt.Sprintf("dump_%s.sql.gz", time.Now().Format("2006-01-02-150405"))
	zipPath := fmt.Sprintf(backupFolder + "/" + zipName)
	zipFile, err := os.Create(zipPath)
	if err != nil {
		log.Fatal(err)
	}

	// Close the dump file after dump is completed
	defer zipFile.Close()

	// Create new Writer to write to Dump file
	wDumpFile := gzip.NewWriter(zipFile)
	// Create new scanner to read from Stdout io pipe
	scanner := bufio.NewScanner(stdOut)

	// Run the DB Dump command
	if err := dumpCmd.Start(); err != nil {
		log.Fatal(err)
	}

	// Read each line from scanner and write the each line (string) to file
	for scanner.Scan() {
		_, err := wDumpFile.Write([]byte(scanner.Text() + "\n"))
		if err != nil {
			log.Fatal(err)
		}
	}
	wDumpFile.Flush()
	wDumpFile.Close()

	fmt.Printf("Created Zipped Dump file: %v\n", zipPath)

	return zipPath
}

func uploadToCloud(zipPath string) {
	srv, err := drive.NewService(
		context.Background(),
		option.WithCredentialsFile("balmy-moonlight-196910-348c8ecd1dca.json"),
	)
	if err != nil {
		log.Fatalf("Unable to retrieve Drive client: %v", err)
	}

	zipFile, err := os.Open(zipPath)
	if err != nil {
		log.Fatal(err)
	}

	zipFileInfo, err := zipFile.Stat()
	if err != nil {
		log.Fatal(err)
	}
	defer zipFile.Close()

	driveFile := &drive.File{
		Name:    filepath.Base(zipFileInfo.Name()),
		Parents: []string{"0B6VheyHPSfoJRjMwbERBdW52ZTQ"},
	}

	fileResult, err := srv.Files.Create(driveFile).Media(zipFile, googleapi.ContentType("text/x-sql")).Do()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%v\n", fileResult)

	wg.Done()
}

func cleanOldLocalDumps(retention *time.Duration) {
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
}

func cleanOldCloudDumps(retention *time.Duration) {
	// Get list of backups older than retention time
	srv, err := drive.NewService(
		context.Background(),
		option.WithCredentialsFile("balmy-moonlight-196910-348c8ecd1dca.json"),
	)
	if err != nil {
		log.Fatalf("Unable to retrieve Drive client: %v", err)
	}

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
}
