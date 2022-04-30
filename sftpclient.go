package main

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"os"
	"strings"
	"time"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
	"gopkg.in/ini.v1"
)

var clients = make([]*SftpClient,0)

type SftpClient struct {
	Host string
	Port string
	Username string
	Password string
	PrivateKey string
	Prefix string
	Suffix string
	Extension string
	Interval int
	Naming string
	Conn *ssh.Client
	Client *sftp.Client
	Config  *ini.File
	Path string
	UsePrivateKey bool
	FilesList []string
	DelayUnits string
}

func  (s *SftpClient)  LoadConfig(confFile string) *ini.File {
	var err error
	s.Config, err = ini.Load(confFile)
	if err != nil {
		fmt.Printf("unable to load config [%v]\n", err)
		os.Exit(1)
	}
	return s.Config
}

func (s *SftpClient) ClientsCount() int {
	return len(clients)
}
func (s *SftpClient) Init(tc string) error {
	var err error
	var testcases string
	sec, _ := s.Config.GetSection("DEFAULT")
	if tc != "" {
		testcases = tc
	} else {
		if sec.HasKey("RUN_TESTCASE") {
			testcases = sec.Key("RUN_TESTCASE").String()
		} else {
			testcases = ""
		}

	}

	for _, testcase := range strings.Split(testcases, ",") {
		testcase = strings.TrimSpace(testcase)
		if testcase == "" {
			continue
		}
		log.Printf("Reading Test Case:%v",testcase)
		sec2, _ := s.Config.GetSection(testcase)
		client := &SftpClient{}
		client.Host = sec2.Key("HOST").String()
		client.Port = sec2.Key("PORT").String()
		client.Username = sec2.Key("USERNAME").String()
		if sec2.HasKey("PASSWORD") {
			client.Password = sec2.Key("PASSWORD").String()
		}
		if sec2.HasKey("PRIVATEKEY") {
			keyContents, err := ioutil.ReadFile(sec2.Key("PRIVATEKEY").String())
			if err == nil {
				client.PrivateKey = string(keyContents)
				client.UsePrivateKey = true
			}
		}

		sec3, _ := s.Config.GetSection(fmt.Sprintf("%v_LOADER",testcase))
		if sec3.HasKey("DELAY") {
			client.Interval, err = sec3.Key("DELAY").Int()
			if err!=nil {
				fmt.Printf("Invalid value for Interval => TestCase:%v\n",testcase)
				fmt.Printf("Skipping")
				continue
			}
		} else {
			client.Interval = 1
		}

		if sec3.HasKey("DELAY_UNITS") {
			client.DelayUnits = sec3.Key("DELAY_UNITS").String()
		} else {
			client.DelayUnits = "Seconds"
		}

		client.Prefix = sec3.Key("FILE_PREFIX").String()
		client.Suffix = sec3.Key("FILE_SUFFIX").String()
		client.Extension = sec3.Key("FILE_EXTENSION").String()
		client.Naming = sec3.Key("FILE_NAME").String()
		if sec3.HasKey("PATH") {
			client.Path = sec3.Key("PATH").String()
		} else {
			client.Path = "."
		}
		if sec3.HasKey("DATADIR") {
			client.FilesList, err = s.RetrieveFilesList(sec3.Key("DATADIR").String())
			if err != nil {
				log.Printf("Error occurred while retrieving files list:%v",err)
				client.FilesList = nil
			}
		}
		clients = append(clients, client)
	}
	return nil
}

func (s *SftpClient) DryRun(fileCount int) {
	count := 0
	completed := false
	for {
		for _, client := range clients {
			now := time.Now()
			filename := fmt.Sprintf("%v/%v_%v_%v_%v.%v",client.Path,client.Prefix,client.Naming,now.Format("20060102150405"),client.Suffix,client.Extension)
			log.Printf("Uploading target file:%v",filename)
			client.PutFile(client.Host,client.Port,client.Username,client.Password,filename)
			if client.DelayUnits == "Seconds" {
				time.Sleep(time.Second*time.Duration(client.Interval))
			} else if client.DelayUnits == "MilliSeconds" {
				time.Sleep(time.Millisecond*time.Duration(client.Interval))
			} else if client.DelayUnits == "MicroSeconds" {
				time.Sleep(time.Microsecond*time.Duration(client.Interval))
			} else if client.DelayUnits == "NanoSeconds" {
				time.Sleep(time.Nanosecond*time.Duration(client.Interval))
			}

			count = count + 1
			if count >= fileCount {
				completed = true
				break
			}
		}
		if completed {
			break
		}

	}

}

func  (s *SftpClient)  GetPublicKey(pubkey string) ssh.AuthMethod {
	var buf bytes.Buffer
	buf.Write([]byte(pubkey))
	key, err := ssh.ParsePrivateKey(buf.Bytes())
	if err != nil {
		return nil
	}
	return ssh.PublicKeys(key)
}

func (s *SftpClient)  ConnectWithPublicKey(host, port, username, pubKey string) error {
	var err error
	sshConfig := &ssh.ClientConfig{
		User: username,
		Auth: []ssh.AuthMethod{
			s.GetPublicKey(pubKey),
		},
		HostKeyCallback: ssh.HostKeyCallback(func(hostname string, remote net.Addr, key ssh.PublicKey) error { return nil }),
	}
	s.Conn, err = ssh.Dial("tcp", fmt.Sprintf("%v:%v",host,port), sshConfig)
	if err != nil {
		return  fmt.Errorf("Failed to dial: %s", err)
	}
	s.Client, err = sftp.NewClient(s.Conn)
	if err != nil {
		return err
	}
	return nil
}

func (s *SftpClient)  ConnectWithPassword(host, port, username, password string) error {
	var err error
	sshConfig := &ssh.ClientConfig{
		User: username,
		Auth: []ssh.AuthMethod{
			ssh.Password(password),
		},
		HostKeyCallback: ssh.HostKeyCallback(func(hostname string, remote net.Addr, key ssh.PublicKey) error { return nil }),
	}
	s.Conn, err = ssh.Dial("tcp", fmt.Sprintf("%v:%v",host,port), sshConfig)
	if err != nil {
		return  fmt.Errorf("Failed to dial: %s", err)
	}
	s.Client, err = sftp.NewClient(s.Conn)
	if err != nil {
		return err
	}
	return nil
}

func (s *SftpClient) Close() {
	if s.Client!=nil {
		s.Client.Close()
		if s.Conn!=nil {
			s.Conn.Close()
		}
	}
}

func (s *SftpClient) PutFile(host,port, username,password, filename string) error {
	var err error
	totalDeliveries += 1
	if s.UsePrivateKey {
		log.Printf("Connecting with Private Key")
		err = s.ConnectWithPublicKey(host,port, username,s.PrivateKey)
	} else {
		log.Printf("Connecting with Password")
		err = s.ConnectWithPassword(host, port,username,password)
	}
	if err != nil {
		log.Printf("SFTP Connection failed to establish:%v",err)
		return err
	}
	if s.FilesList != nil {
		randomIndex := rand.Intn(len(s.FilesList))
		srcFile := s.FilesList[randomIndex]
		log.Printf("Uploading source file:%v",srcFile)
		fr, ferr := os.Open(srcFile)
		if ferr!=nil {
			log.Printf("Failed to retrieve source file:%v",srcFile)
			return ferr
		}
		defer fr.Close()

		fw, ferr2 := s.Client.Create(filename)
		if ferr2 != nil {
			log.Printf("Failed to create target file  on server:%v",filename)
			return ferr2
		}
		defer fw.Close()
		n, cErr := io.Copy(fw, fr)
		if cErr!=nil {
			log.Printf("Failed to upload file %v to SFTP Server:%v",filename,s.Host)
			return err
		}
		log.Printf("Successfully uploaded file:%v with Size:%v",filename,n)
		successfulDeliveries += 1
		return nil
	}
	if err == nil {
		fw, err := s.Client.Create(filename)
		if err!=nil {
			log.Printf("Failed to create target file:%v",filename)
			return err
		}
		fw.Write([]byte("This is just for testing"))
		fw.Close()
		successfulDeliveries += 1
	} else {
		log.Printf("Error occurred:%v",err)
	}
	s.Close()
	return err
}