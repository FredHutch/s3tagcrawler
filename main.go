package main

/*
NOTE: If you get a "Too many open files" error when running this, change your ulimit:
oldulimit=$(ulimit -n)
ulimit -n 4096
ulimit -n $oldulimit
TODO: tune the program so this is not needed.
*/

// FIXME if a file already exists in S3, don't overwrite it, print a message
// with the object name and go on to the next.

import (
	"encoding/csv"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net/url"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ncw/swift"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/google/uuid"
	"gopkg.in/alecthomas/kingpin.v2"
)

const (
	// TAG is a constant for 'tag'
	TAG = "tag"
)

var (
	tagOnly = kingpin.Flag("tag-only", "Just tag, don't upload").Short('t').Bool()
	csvFile = kingpin.Arg("csv", "CSV file").Required().String()
	// technically, seq_dir is not required when the -t flag is used
	// but for now we are requiring it. FIXME ?
	requiredColumns = []string{"seq_dir", "s3transferbucket", "s3_prefix", "data_type"}
	swiftConnection = swift.Connection{}
)

// DataType determines whether this is array or sequencing data
type DataType string

// Data types
const (
	ArrayData      DataType = "0"
	SequencingData DataType = "1"
)

// CSVRecord represents a row in the csv manifest
type CSVRecord struct { // second iteration: 17-12-24-TagManifestUpdated.csv
	localDir         string
	s3TransferBucket string
	s3Prefix         string
	dataType         DataType
	tags             map[string]string
	isInSwift        bool
	swiftContainer   string
	swiftPath        string
}

func asString(tags []*s3.Tag) string {
	var strangs []string
	for _, tag := range tags {
		strangs = append(strangs, fmt.Sprintf("%s=%s", *tag.Key, *tag.Value))
	}
	return strings.Join(strangs, "&")
}

// file: seq_dir,s3transferbucket,s3_prefix,molecular_id,assay_material_id,stage
// struct: molecular_id,assay_material_id,s3transferbucket,s3_prefix,seq_dir,,,,,stage

// Uploads 100 files with random-ish tags
func uploadFilesWithTags() {
	svc := s3.New(session.New())
	bucket := "dtenenba-test"
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			GGworkflowID := rand.Intn(20)
			assayMaterialID := rand.Intn(15)
			// fmt.Print(".")
			molecularID := rand.Intn(17)
			objectName := uuid.New().String()
			tags := fmt.Sprintf("stage=raw&GGworkflowId=%d&assayMaterialID=%d&molecularID=%d",
				GGworkflowID, assayMaterialID, molecularID)

			input := &s3.PutObjectInput{
				Body:   aws.ReadSeekCloser(strings.NewReader("filetoupload")),
				Bucket: aws.String(bucket),
				Key:    aws.String(objectName),
				// ServerSideEncryption: aws.String("AES256"), // FIXME use this in HSE
				Tagging: aws.String(tags),
			}

			_, err := svc.PutObject(input)
			if err != nil {
				if aerr, ok := err.(awserr.Error); ok {
					switch aerr.Code() {
					default:
						fmt.Println(aerr.Error())
					}
				} else {
					// Print the error, cast err to awserr.Error to get the Code and
					// Message from an error.
					fmt.Println(err.Error())
				}
				// return
			}

			fmt.Printf("Uploading %s with tags %s...\n", objectName, tags)

		}()
		wg.Wait()
	}
}

func deleteFile(fileToUpload string, rec CSVRecord, wg *sync.WaitGroup, svc s3.S3, ops *uint64) {
	defer wg.Done()
	obj := rec.s3Prefix + fileToUpload
	_, derr := svc.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(rec.s3TransferBucket),
		Key:    aws.String(obj),
	})
	if derr != nil {
		panic(derr)
	}
	atomic.AddUint64(ops, 1)
}

func fileExistsInS3(fileName string, rec CSVRecord, svc s3.S3) bool {
	res, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket:    aws.String(rec.s3TransferBucket),
		Delimiter: aws.String("/"),
		Prefix:    aws.String(rec.s3Prefix + fileName),
	})
	if err != nil {
		panic(err)
	}
	// fmt.Println("fileExistsInS3 got result", res)
	return *res.KeyCount > 0
}

func uploadFile(fileToUpload string, rec CSVRecord, wg *sync.WaitGroup, svc s3.S3, ops *uint64, guard chan struct{}) {
	defer wg.Done()
	defer func() { <-guard }()
	if fileExistsInS3(fileToUpload, rec, svc) {
		fmt.Println(rec.s3Prefix+fileToUpload, "already exists in S3; not overwriting.")
		return
	}
	var fd io.ReadSeeker
	if rec.isInSwift {
		st, err := os.Lstat(rec.localDir)
		if err != nil {
			panic(err)
		}
		if st.IsDir() {
			fd, err = os.Open(rec.localDir + fileToUpload)
		} else {
			fd, err = os.Open(rec.localDir)
		}
		if err != nil {
			panic(err)
		}
		// defer fd.Close()
	} else {
		if swiftConnection.AuthToken == "" || swiftConnection.StorageUrl == "" {
			swiftConnection.AuthToken = os.Getenv("OS_AUTH_TOKEN")
			swiftConnection.StorageUrl = os.Getenv("OS_STORAGE_URL")
			if swiftConnection.AuthToken == "" || swiftConnection.StorageUrl == "" {
				fmt.Println("Environment variables OS_AUTH_TOKEN and OS_STORAGE_URL must be set in order to use Swift!")
				os.Exit(1)
			}
		}
		var err error
		fd, _, err = swiftConnection.ObjectOpen(rec.swiftContainer,
			fileToUpload, false, swift.Headers{})
		if err != nil {
			panic(err)
		}
	}
	v := url.Values{}
	for key, value := range rec.tags {
		v.Set(key, value)
	}

	// FIXME add omicsSampleName here?
	input := &s3.PutObjectInput{
		Bucket:  aws.String(rec.s3TransferBucket),
		Key:     aws.String(rec.s3Prefix + fileToUpload),
		Body:    fd,
		Tagging: aws.String(v.Encode()),
	}
	_, perr := svc.PutObject(input)
	if perr != nil {
		if aerr, ok := perr.(awserr.Error); ok {
			switch aerr.Code() {
			default:
				fmt.Println(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			fmt.Println(perr.Error())
		}
		// return
	}
	atomic.AddUint64(ops, 1)
	fmt.Printf("Uploading %s%s with tags %s...\n", rec.s3Prefix, fileToUpload, v.Encode())

}

//
func tagFile(fileToUpload string, rec CSVRecord, wg *sync.WaitGroup, svc s3.S3, ops *uint64, guard chan struct{}) {
	defer wg.Done()
	defer func() { <-guard }()

	var tags []*s3.Tag
	for k, v := range rec.tags {
		tag := s3.Tag{Key: aws.String(k), Value: aws.String(v)}
		tags = append(tags, &tag)
	}

	input := &s3.PutObjectTaggingInput{
		Bucket: aws.String(rec.s3TransferBucket),
		Key:    aws.String(fileToUpload),
		Tagging: &s3.Tagging{
			TagSet: tags,
		},
	}
	_, perr := svc.PutObjectTagging(input)
	if perr != nil {
		if aerr, ok := perr.(awserr.Error); ok {
			switch aerr.Code() {
			default:
				fmt.Println(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			fmt.Println(perr.Error())
		}
		// return
	}
	atomic.AddUint64(ops, 1)
	fmt.Printf("Tagging %s%s with tags %s...\n", rec.s3Prefix, fileToUpload, asString(input.Tagging.TagSet))

}

//

// MyFileInfo is my bogus implementation of FileInfo
type MyFileInfo string

// Name is bogus
func (m MyFileInfo) Name() string {
	return string(m)
}

// Size is bogus
func (m MyFileInfo) Size() int64 {
	return 0
}

// Mode is bogus
func (m MyFileInfo) Mode() os.FileMode {
	return 0
}

// ModTime is bogus
func (m MyFileInfo) ModTime() time.Time {
	return time.Now()
}

// IsDir is bogus
func (m MyFileInfo) IsDir() bool {
	return false
}

// Sys is bogus
func (m MyFileInfo) Sys() interface{} {
	return nil
}

func getRecord(record []string, headers map[int]string) CSVRecord {
	rec := CSVRecord{}
	var tags map[string]string
	tags = make(map[string]string)

	for i, item := range record {
		columnName := headers[i]
		if isStringInSlice(columnName, requiredColumns) {
			switch columnName {
			case "seq_dir":
				rec.localDir = item
				if strings.HasPrefix(item, "swift://") {
					rec.isInSwift = true
					url, err := url.Parse(item)
					if err != nil {
						fmt.Println("invalid url: ", item)
						os.Exit(1)
					}
					rec.swiftContainer = url.Host
					rec.swiftPath = strings.TrimLeft(url.Path, "/")
				}
			case "s3transferbucket":
				rec.s3TransferBucket = item
			case "s3_prefix":
				rec.s3Prefix = item
			case "data_type":
				rec.dataType = DataType(item)
			}
		} else {
			tags[columnName] = item
		}
	}
	rec.tags = tags
	return rec
}

func handleRecord(record []string, headers map[int]string, wg *sync.WaitGroup, svc s3.S3, cmd string, ops *uint64, guard chan struct{}) {
	rec := getRecord(record, headers)
	var file os.FileInfo

	var files []os.FileInfo

	var uploadSingleFile bool

	if !strings.HasSuffix(rec.s3Prefix, "/") {
		rec.s3Prefix = rec.s3Prefix + "/"
	}

	if cmd == TAG {

		// first see if rec.s3Prefix refers to an object, or is just a prefix

		unslashed := strings.TrimRight(rec.s3Prefix, "/")
		// unslashed := rec.s3Prefix // TODO remove!
		headInput := &s3.HeadObjectInput{
			Bucket: aws.String(rec.s3TransferBucket),
			Key:    aws.String(unslashed),
		}
		_, err := svc.HeadObject(headInput)
		if err != nil {
			if aerr, ok := err.(awserr.Error); ok {
				switch aerr.Code() {
				default:
					if aerr.Message() == "Not Found" {
						// object does not exist
					}
				}
			} else {
				panic(err)
			}
		} else {
			rec.s3Prefix = unslashed
		}

		input := &s3.ListObjectsV2Input{
			Bucket: aws.String(rec.s3TransferBucket),
			// Delimiter: aws.String("/"),
			Prefix:  aws.String(rec.s3Prefix),
			MaxKeys: aws.Int64(99999),
		}
		res, err := svc.ListObjectsV2(input)
		if err != nil {
			panic(err)
		}

		if len(res.Contents) == 1 {
			uploadSingleFile = true
		}

		for _, item := range res.Contents {
			key := *item.Key
			var mfi = MyFileInfo(key)
			files = append(files, mfi)
		}

	} else {
		// FIXME TODO DANTE handle it if file to upload is in swift
		f, err := os.Lstat(rec.localDir)
		if err != nil {
			panic(err)
		}

		if f.IsDir() {
			if !strings.HasSuffix(rec.localDir, "/") {
				rec.localDir = rec.localDir + "/"
			}

			files, err = ioutil.ReadDir(rec.localDir)
			if err != nil {
				panic(err)
			}
		} else {
			uploadSingleFile = true
			files = append(files, f)
		}

	}

	for _, file = range files {
		if file.IsDir() {
			continue
		}
		if rec.dataType == SequencingData {
			// fastq

			if (!uploadSingleFile) &&
				!(strings.HasSuffix(file.Name(), ".fastq") ||
					strings.HasSuffix(file.Name(), ".fastq.gz")) {
				continue
			}
		}

		// }

		switch cmd {
		case "upload":
			wg.Add(1)
			guard <- struct{}{}
			go uploadFile(file.Name(), rec, wg, svc, ops, guard)
		case TAG:
			wg.Add(1)
			guard <- struct{}{}
			go tagFile(file.Name(), rec, wg, svc, ops, guard)
		case "delete":
			deleteFile(file.Name(), rec, wg, svc, ops)
		case "count":
			atomic.AddUint64(ops, 1)
		}
	}
}

func isStringInSlice(str string, sl []string) bool {
	for _, item := range sl {
		if item == str {
			return true
		}
	}
	return false
}

func getHeaders(record []string) map[int]string {
	// make sure all required columns are present, and there
	// must be at least one additional column (for tagging)
	requiredCount := 0
	var headers map[int]string
	headers = make(map[int]string)
	for i, item := range record {
		if isStringInSlice(item, requiredColumns) {
			requiredCount++
		}
		headers[i] = item
	}

	if requiredCount < len(requiredColumns) {
		fmt.Println("Missing required column(s)!")
		os.Exit(1)
	}
	if len(headers) == len(requiredColumns) {
		fmt.Println("No tag columns!")
		os.Exit(1)
	}
	return headers
}

func main() {
	kingpin.CommandLine.Help = `
Please supply the name of a csv file.
The file must contain the following columns:

seq_dir - the directory of files to upload

s3transferbucket - the name of the S3 bucket to upload to

s3_prefix - a prefix in S3 under which to upload

data_type - set to 0 to upload all files and 1 to upload fastq/fastq.gz only

You must supply at least one additional column containing the 
tag(s) you want to apply.

NOTE: This program will use all available cores. Be a good citizen and
'grabnode' so that this program does not disrupt others' work.
More information:
https://teams.fhcrc.org/sites/citwiki/SciComp/Pages/Grab%%20Commands.aspx

Full documentation is here:
https://github.com/FredHutch/s3tagcrawler/

`
	kingpin.Parse()

	var ops uint64
	maxGoroutines := runtime.NumCPU() - 1
	guard := make(chan struct{}, maxGoroutines)

	cmd := "upload"
	if *tagOnly {
		cmd = TAG
	}
	os.Setenv("AWS_REGION", "us-west-2") // FIXME
	svc := s3.New(session.New())

	fileHandle, err := os.Open(*csvFile)
	if err != nil {
		panic(err)
	}
	defer fileHandle.Close()
	r := csv.NewReader(fileHandle)
	var wg sync.WaitGroup
	var headers map[int]string
	for i := 0; ; i++ {
		record, readErr := r.Read()
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			log.Fatal(readErr)
		}
		if i == 0 {
			headers = getHeaders(record)
			continue
		}
		handleRecord(record, headers, &wg, *svc, cmd, &ops, guard)
	}
	wg.Wait()
	fmt.Println("Number of ops:", atomic.LoadUint64(&ops))
}
