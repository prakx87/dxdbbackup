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
	"time"

	"google.golang.org/api/drive/v3"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

var databases = []string{}
var dumpList = []string{}

func main() {
	fmt.Println("DX DB Backup Script")

	// 1. List DBs for backups excluding default DBs
	getDbList()

	// 2. Take DB dumps of mysql databases
	takeDump()

	// 3. Upload the DB dumps to Google Drive
	uploadToCloud()

	// 4. Cleanup older dumps
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

	for _, db := range allDbs {
		if !slices.Contains(defaultDbs, db) {
			databases = append(databases, db)
		}
	}

	fmt.Printf("List of Databases to take backup: %v\n", databases)
	return databases
}

func takeDump() {
	//  mysqldump -h 127.0.0.1 -u root -pmy-secret-pw semaphore > dump.sql
	for _, db := range databases {
		dumpCmd := exec.Command("/usr/bin/mysqldump", "-h", "127.0.0.1", "-uroot", "-pmy-secret-pw", db)
		// Create stdout io pipe in order to write output to file
		stdOut, err := dumpCmd.StdoutPipe()
		if err != nil {
			log.Fatal(err)
		}

		// Create dump file
		dumpName := fmt.Sprintf("dump_%s.sql", time.Now().Format("2006-01-02-150405"))
		dumpFile, err := os.Create(dumpName)
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

		dumpList = append(dumpList, dumpName)
	}
}

func uploadToCloud() {
	for _, dumpName := range dumpList {
		srv, err := drive.NewService(
			context.Background(),
			option.WithCredentialsFile("balmy-moonlight-196910-348c8ecd1dca.json"),
		)
		if err != nil {
			log.Fatalf("Unable to retrieve Drive client: %v", err)
		}

		dumpFile, err := os.Open(dumpName)
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
	}

}
