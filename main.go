package main

import (
	"flag"
	"fmt"
	"log"
	"os"
)
var (
	conf string
	fileCount int
	testcase string
	successfulDeliveries int
	totalDeliveries int
	version bool
	APP_VERSION="v2022.04.01"
)


func main() {
	flag.StringVar(&conf,"conf","","Config file")
	flag.IntVar(&fileCount,"filecount",1,"File Count")
	flag.StringVar(&testcase,"testcase","","Test case")
	flag.BoolVar(&version,"version",false, "App Version")
	flag.Parse()
	if version  {
		fmt.Printf("%v",APP_VERSION)
		os.Exit(0)
	}
	log.Printf("Running Dry Run with config %v",conf)
	successfulDeliveries = 0
	totalDeliveries = 0
	mgr := &SftpClient{}
	mgr.LoadConfig(conf)
	mgr.Init(testcase)
	count := mgr.ClientsCount()
	if count > 0 {
		mgr.DryRun(fileCount)
		log.Printf("%v out of %v files delivered successfully",successfulDeliveries,totalDeliveries)
	} else {
		log.Printf("No sftp client connections defined")
	}


}
