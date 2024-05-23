package local

import (
	"bytes"
	"fmt"
	"log"
	"os/exec"
	"slices"
	"strings"
)

type Dbcreds struct {
	db       string
	host     string
	user     string
	password string
	port     uint16
}

func GetDbList(dbCreds *Dbcreds) []string {
	// mysql -h 127.0.0.1 -u root -pmy-secret-pw -e 'show databases;'
	cmd := exec.Command("/usr/bin/mysql", "-h", dbCreds.host, "-u", dbCreds.user, "-pmy-secret-pw")

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
