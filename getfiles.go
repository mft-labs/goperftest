package main

import (
	"log"
	"os"
	"path/filepath"
)

func (s *SftpClient) RetrieveFilesList(dir string) ([]string, error) {
	filesList := make([]string, 0)
	walkErr := filepath.Walk(dir, func(path string, info os.FileInfo, e error) error {
		if e != nil {
			return nil
		}

		if info.Mode().IsRegular() {
			log.Printf("Adding file path:%v",path)
			filesList = append(filesList, path)
		}
		return nil
	})
	return filesList, walkErr
}
