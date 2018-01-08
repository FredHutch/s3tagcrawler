package main

/*
NOTE: If you get a "Too many open files" error when running this, change your ulimit:
oldulimit=$(ulimit -n)
ulimit -n 4096
ulimit -n $oldulimit
TODO: tune the program so this is not needed.
*/

import (
	"encoding/csv"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/url"
	"strings"
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

			result, err := svc.PutObject(input)
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
			fmt.Println(result)

		}()
		wg.Wait()
	}
}

func uploadFile(fileToUpload string, rec CSVRecord, wg *sync.WaitGroup, svc s3.S3) {
	defer wg.Done()
	fd, err := os.Open(rec.localDir + fileToUpload)
	if err != nil {
		panic(err)
	}
	defer fd.Close()
	v := url.Values{}
	v.Set("stage", rec.stage)
	v.Set("assayMaterialId", rec.assayMaterialID)
	v.Set("molecularID", rec.molecularID)

	fmt.Println("v is", v.Encode())
	if true {
		return
	}

	// FIXME add omicsSampleName here?
	input := &s3.PutObjectInput{
		Bucket:  aws.String(rec.s3TransferBucket),
		Key:     aws.String(rec.s3Prefix + fileToUpload),
		Body:    fd,
		Tagging: aws.String(v.Encode()),
	}
	result, err := svc.PutObject(input)
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
	fmt.Printf("Uploading %s%s with tags %s...\n", rec.s3Prefix, fileToUpload, v.Encode())
	fmt.Println(result)

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

func handleRecord(record []string, wg *sync.WaitGroup, svc s3.S3) {
	defer wg.Done()
	var uploadWg sync.WaitGroup
	rec := CSVRecord{molecularID: record[3], assayMaterialID: record[4],
		s3TransferBucket: record[1], s3Prefix: record[2], localDir: record[0],
		stage: record[5]}

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
		if !file.IsDir() {
			// TODO - need additional tests for .fastq/.fastq.gz suffix
			// or omicsSampleName prefix?
			uploadWg.Add(1)
			go uploadFile(file.Name(), rec, &uploadWg, svc)
		}
	}
	uploadWg.Wait()

	// err := svc.ListObjectsV2Pages(&s3.ListObjectsV2Input{
	// 	Bucket:    &rec.s3TransferBucket,
	// 	Delimiter: aws.String("/"),
	// 	Prefix:    &rec.s3Prefix,
	// }, func(o *s3.ListObjectsV2Output, lastPage bool) bool {
	// 	for i := 0; i < len(o.Contents); i++ {
	// 		obj := o.Contents[i].Key
	// segs := strings.Split(*obj, "/")
	// fileName := segs[len(segs)-1]
	// if strings.HasPrefix(fileName, rec.omicsSampleName) &&
	// 	(strings.HasSuffix(fileName, ".fastq") ||
	// strings.HasSuffix(fileName, ".fastq.gz")) {
	// fmt.Println("got a file", *obj, "filename is ", fileName)
	// 		tagwg.Add(1)
	// 		// fmt.Println("loquat")
	// 		go tagRecord(obj, rec, &tagwg, svc)
	// 		// }
	// 	}
	// 	return !*o.IsTruncated
	// 	// return lastPage
	// })
	// if err != nil {
	// 	panic(err)
	// }
	// fmt.Println("let's wait")
	// tagwg.Wait()
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Supply the name of a csv file!")
		os.Exit(1)
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
		// fmt.Println("got a record!", record)
		wg.Add(1)
		go handleRecord(record, &wg, *svc)
	}
	// fmt.Println("made it to the end")
	wg.Wait()
	// fmt.Println("really done")
}
