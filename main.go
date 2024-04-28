package main

import (
	"bufio"
	"bytes"
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

	// 2. Take DB dumps of mysql databases
	dumpList := takeDump(databases)

	// 3. Upload the DB dumps to Google Drive
	wg.Add(len(dumpList))
	for _, dumpPath := range dumpList {
		go uploadToCloud(dumpPath)
	}

	// 4. Cleanup older dumps
	retentionTime := "15h"
	cleanOldLocalDumps(retentionTime)
	cleanOldCloudDumps(retentionTime)

	wg.Wait()
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

func takeDump(databases []string) []string {
	//  mysqldump -h 127.0.0.1 -u root -pmy-secret-pw semaphore > dump.sql
	dumpList := []string{}
	for _, db := range databases {
		dumpCmd := exec.Command("/usr/bin/mysqldump", "-h", "127.0.0.1", "-uroot", "-pmy-secret-pw", db)
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

		dumpName := fmt.Sprintf("dump_%s.sql", time.Now().Format("2006-01-02-150405"))
		dumpPath := fmt.Sprintf(backupFolder + "/" + dumpName)
		dumpFile, err := os.Create(dumpPath)
		if err != nil {
			log.Fatal(err)
		}

		// Create new Writer to write to Dump file
		wDumpFile := bufio.NewWriter(dumpFile)
		// Create new scanner to read from Stdout io pipe
		scanner := bufio.NewScanner(stdOut)

		// Run the DB Dump command
		if err := dumpCmd.Start(); err != nil {
			log.Fatal(err)
		}

		// Read each line from scanner and write the each line (string) to file
		for scanner.Scan() {
			if _, err := wDumpFile.WriteString(scanner.Text() + "\n"); err != nil {
				log.Fatal(err)
			}
		}

		// Close the dump file after dump is completed
		dumpFile.Close()

		dumpList = append(dumpList, dumpPath)
	}
	return dumpList
}

func uploadToCloud(dumpPath string) {
	srv, err := drive.NewService(
		context.Background(),
		option.WithCredentialsFile("balmy-moonlight-196910-348c8ecd1dca.json"),
	)
	if err != nil {
		log.Fatalf("Unable to retrieve Drive client: %v", err)
	}

	dumpFile, err := os.Open(dumpPath)
	if err != nil {
		log.Fatal(err)
	}

	dumpFileInfo, err := dumpFile.Stat()
	if err != nil {
		log.Fatal(err)
	}
	defer dumpFile.Close()

	driveFile := &drive.File{
		Name:    filepath.Base(dumpFileInfo.Name()),
		Parents: []string{"0B6VheyHPSfoJRjMwbERBdW52ZTQ"},
	}

	fileResult, err := srv.Files.Create(driveFile).Media(dumpFile, googleapi.ContentType("text/x-sql")).Do()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%v\n", fileResult)

	wg.Done()
}

func cleanOldLocalDumps(retentionTime string) {
	// Get list of backups older than retention time
	retention, err := time.ParseDuration(retentionTime)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Retention Time: %v\n", retention)

	backupList, err := os.ReadDir("backups")
	if err != nil {
		log.Fatal(err)
	}
	for _, backup := range backupList {
		backupInfo, err := backup.Info()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Name: %v, Info: %v\n", backup.Name(), backupInfo.ModTime())

		backupAge := time.Since(backupInfo.ModTime())
		fmt.Printf("Backup Age: %v\n", backupAge.Abs())
		if backupAge > retention {
			err := os.Remove("backups/" + backup.Name())
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("File %v is deleted.\n", backup.Name())
		}
	}
	// Delete those backups
	fmt.Printf("cleaned backups older than %v.\n", retentionTime)
}

func cleanOldCloudDumps(retentionTime string) {
	// Get list of backups older than retention time
	// Delete those backups
	fmt.Printf("cleaned backups older than %v.\n", retentionTime)
}
