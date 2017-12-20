package main

import (
	"fmt"
	"strings"
	// "github.com/Pallinder/go-randomdata"
	// "github.com/aws/aws-sdk-go/aws/session"
	// "github.com/aws/aws-sdk-go/service/s3"
	"math/rand"

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

func main() {
	// svc := s3.New(session.New(aws.NewConfig().WithLogLevel(aws.LogDebugWithHTTPBody)))
	svc := s3.New(session.New())
	// toCh := make(chan string)
	// fromCh := make(chan DBrecord)
	err := svc.ListObjectsPages(&s3.ListObjectsInput{
		Bucket:    aws.String("dtenenba-test"),
		Delimiter: aws.String("/"),
		// MaxKeys:   aws.Int64(1000),
	}, func(p *s3.ListObjectsOutput, lastPage bool) bool {
		// fmt.Println("items", len(p.Contents), aws.StringValue(p.NextMarker), aws.BoolValue(p.IsTruncated))
		for i := 0; i < len(p.Contents); i++ {
			// fmt.Println(*p.Contents[i].Key)
			// go func() {
			result, err := svc.GetObjectTagging(&s3.GetObjectTaggingInput{
				Bucket: aws.String("dtenenba-test"),
				Key:    aws.String(*p.Contents[i].Key),
			})
			if err != nil {
				fmt.Println(err)
				panic("oops")
			}
			// fmt.Printf("There are %d tags for the key %s.\n", len(result.TagSet), *p.Contents[i].Key)
			for j := 0; j < len(result.TagSet); j++ {
				tag := result.TagSet[j]
				//fmt.Printf("%s\t%s\t%s\t%s\n", "dtenenba-test", aws.String(tag.Key), tag.Value, *p.Contents[i].Key)
				rec := DBrecord{
					bucketname: "dtenenba-test",
					tagKey:     *tag.Key,
					tagValue:   *tag.Value,
					objectKey:  *p.Contents[i].Key,
				}

				// fmt.Println("tkey", *tag.Key, "tvalue", *tag.Value, "bkey", *p.Contents[i].Key)

				fmt.Printf("%s\t%s\t%s\t%s\n", rec.bucketname, rec.tagKey, rec.tagValue, rec.objectKey)
			}
			// }()
			// toCh <- *p.Contents[i].Key
		}
		return *p.IsTruncated
	})
	if err != nil {
		fmt.Println("error", err)
		return
	}
}
