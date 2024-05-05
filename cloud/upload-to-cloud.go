package cloud

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"

	"google.golang.org/api/drive/v3"
	"google.golang.org/api/googleapi"
)

func UploadToCloud(srv drive.Service, zipPath string, wg *sync.WaitGroup) {

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

	defer wg.Done()
}
