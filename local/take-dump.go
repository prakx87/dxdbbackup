package local

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

func TakeDump(database string) string {
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
