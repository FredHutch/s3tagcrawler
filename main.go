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
	"net/url"
	"runtime"
	"strings"
	"sync/atomic"
	// "github.com/Pallinder/go-randomdata"
	// "github.com/aws/aws-sdk-go/service/s3"
	"math/rand"
	"os"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/google/uuid"
)

// DBrecord represents a row in the database
type DBrecord struct {
	bucketname string
	tagKey     string
	tagValue   string
	objectKey  string
}

// CSVRecord represents a row in the csv manifest
// type CSVRecord struct { // first iteration: manifestForObjTagging.csv
// 	molecularID      string
// 	assayMaterialID  string
// 	s3TransferBucket string
// 	s3Prefix         string
// 	omicsSampleName  string
// }

// CSVRecord represents a row in the csv manifest
type CSVRecord struct { // second iteration: 17-12-24-TagManifestUpdated.csv
	molecularID     string
	assayMaterialID string
	// omicsSampleName  string
	s3TransferBucket string
	s3Prefix         string
	localDir         string
	stage            string
	omicsSampleName  string
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
	fd, err := os.Open(rec.localDir + fileToUpload)
	if err != nil {
		panic(err)
	}
	defer fd.Close()
	v := url.Values{}
	v.Set("stage", rec.stage)
	v.Set("assayMaterialId", rec.assayMaterialID)
	v.Set("molecularID", rec.molecularID)
	v.Set("omicsSampleName", rec.omicsSampleName)

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

func tagRecord(obj *string, rec CSVRecord, tagwg *sync.WaitGroup, svc s3.S3) {
	defer tagwg.Done()
	input := &s3.PutObjectTaggingInput{
		Bucket: aws.String(rec.s3TransferBucket),
		Key:    aws.String(*obj),
		Tagging: &s3.Tagging{
			TagSet: []*s3.Tag{
				{
					Key:   aws.String("molecular_id"),
					Value: aws.String(rec.molecularID),
				},
				{
					Key:   aws.String("assay_material_id"),
					Value: aws.String(rec.assayMaterialID),
				},
				// {
				// 	Key:   aws.String("omics_sample_name"),
				// 	Value: aws.String(rec.omicsSampleName),
				// },
				// FIXME in future 'stage' will be a column in the csv, not hardcoded like
				// here. Some files (those processed by globus) will have stage=processed.
				{
					Key:   aws.String("stage"),
					Value: aws.String("raw"),
				},
			},
		},
	}
	_, err := svc.PutObjectTagging(input)
	if err != nil {
		fmt.Printf("Error tagging %s: %s.\n", *obj, err)
	}
}

func handleRecord(record []string, wg *sync.WaitGroup, svc s3.S3, cmd string, ops *uint64, guard chan struct{}) {
	rec := CSVRecord{molecularID: record[3], assayMaterialID: record[4],
		s3TransferBucket: record[1], s3Prefix: record[2], localDir: record[0],
		stage: record[5], omicsSampleName: strings.TrimSpace(record[6])}

	if !strings.HasSuffix(rec.s3Prefix, "/") {
		rec.s3Prefix = rec.s3Prefix + "/"
	}
	if !strings.HasSuffix(rec.localDir, "/") {
		rec.localDir = rec.localDir + "/"
	}

	files, err := ioutil.ReadDir(rec.localDir)
	if err != nil {
		panic(err)
	}
	for _, file := range files {
		if !file.IsDir() && (strings.HasSuffix(file.Name(), ".fastq") ||
			strings.HasSuffix(file.Name(), ".fastq.gz")) {
			switch cmd {
			case "upload":
				wg.Add(1)
				guard <- struct{}{}
				go uploadFile(file.Name(), rec, wg, svc, ops, guard)
			case "delete":
				deleteFile(file.Name(), rec, wg, svc, ops)
			case "count":
				// fmt.Printf("%s%s\n", rec.localDir, file.Name())
				atomic.AddUint64(ops, 1)
			}
		}
	}
}

func main() {

	var ops uint64
	maxGoroutines := runtime.NumCPU() - 1
	guard := make(chan struct{}, maxGoroutines)

	if len(os.Args) < 2 {
		fmt.Printf(`
Please supply the name of a csv file containing the following a header line
like this:

seq_dir,s3transferbucket,s3_prefix,molecular_id,assay_material_id,stage,omics_sample_name

NOTE: This program will use all available cores. Be a good citizen and
'grablargenode' so that this program does not disrupt others' work.
More information:
https://teams.fhcrc.org/sites/citwiki/SciComp/Pages/Grab%%20Commands.aspx
`)
		os.Exit(1)
	}
	cmd := "upload"
	if len(os.Args) > 2 {
		cmd = os.Args[2]
	}
	os.Setenv("AWS_REGION", "us-west-2") // FIXME
	svc := s3.New(session.New())

	fileHandle, err := os.Open(os.Args[1])
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
