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
	molecularID      string
	assayMaterialID  string
	s3TransferBucket string
	s3Prefix         string
	localDir         string
	stage            string
	omicsSampleName  string
	dataType         DataType
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
	st, err := os.Lstat(rec.localDir)
	if err != nil {
		panic(err)
	}
	var fd *os.File
	if st.IsDir() {
		fd, err = os.Open(rec.localDir + fileToUpload)
	} else {
		fd, err = os.Open(rec.localDir)
	}
	if err != nil {
		panic(err)
	}
	defer fd.Close()
	v := url.Values{}
	v.Set("stage", rec.stage)
	v.Set("assay_material_id", rec.assayMaterialID)
	v.Set("molecular_id", rec.molecularID)
	v.Set("omics_sample_name", rec.omicsSampleName)

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
	input := &s3.PutObjectTaggingInput{
		Bucket: aws.String(rec.s3TransferBucket),
		Key:    aws.String(fileToUpload),
		Tagging: &s3.Tagging{
			TagSet: []*s3.Tag{
				{
					Key:   aws.String("stage"),
					Value: aws.String(rec.stage),
				},
				{
					Key:   aws.String("assay_material_id"),
					Value: aws.String(rec.assayMaterialID),
				},
				{
					Key:   aws.String("molecular_id"),
					Value: aws.String(rec.molecularID),
				},
				{
					Key:   aws.String("omics_sample_name"),
					Value: aws.String(rec.omicsSampleName),
				},
			},
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

func handleRecord(record []string, wg *sync.WaitGroup, svc s3.S3, cmd string, ops *uint64, guard chan struct{}) {
	rec := CSVRecord{molecularID: record[3], assayMaterialID: record[4],
		s3TransferBucket: record[1], s3Prefix: record[2], localDir: record[0],
		stage: record[5], omicsSampleName: record[6],
		dataType: DataType(strings.TrimSpace(record[7]))}

	var file os.FileInfo

	var files []os.FileInfo

	var uploadSingleFile bool

	if !strings.HasSuffix(rec.s3Prefix, "/") {
		rec.s3Prefix = rec.s3Prefix + "/"
	}

	if cmd == TAG {

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
			var mfi MyFileInfo = MyFileInfo(key)
			files = append(files, mfi)
		}

	} else {
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

func main() {
	kingpin.CommandLine.Help = `
Please supply the name of a csv file containing the following a header line
like this:

seq_dir,s3transferbucket,s3_prefix,molecular_id,assay_material_id,stage,omics_sample_name,data_type

NOTE: This program will use all available cores. Be a good citizen and
'grabnode' so that this program does not disrupt others' work.
More information:
https://teams.fhcrc.org/sites/citwiki/SciComp/Pages/Grab%%20Commands.aspx

Full documentation is here:
https://github.com/FredHutch/s3tagcrawler/tree/feature/uploadAndTag

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
	for i := 0; ; i++ {
		record, readErr := r.Read()
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			log.Fatal(readErr)
		}
		if i == 0 { // skip header
			continue
		}
		handleRecord(record, &wg, *svc, cmd, &ops, guard)
	}
	wg.Wait()
	fmt.Println("Number of ops:", atomic.LoadUint64(&ops))
}
