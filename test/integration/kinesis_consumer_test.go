package integration_test

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
)

var (
	// The current integration test docker image only supports 1 stream saidsef/aws-kinesis-local
	fakeAWSID     = "id"
	fakeAWSSecret = "secret"
	fakeAWSToken  = "token"
	fakeAWSRegion = "us-west-2"
)

type TestKinesisConsumer struct {
	kineses *kinesis.Client
}

func NewTestKinesisConsumer(host string, streamNames []string) (*TestKinesisConsumer, error) {
	provider := credentials.NewStaticCredentialsProvider(fakeAWSID, fakeAWSID, "")

	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithBaseEndpoint("http://kinesis:4566"),
		config.WithHTTPClient(&http.Client{Timeout: 10 * time.Second}),
		config.WithRegion(fakeAWSRegion),
		config.WithCredentialsProvider(provider),
	)

	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}
	fmt.Println("host.", *cfg.BaseEndpoint)
	//t := &TestKinesisConsumer{
	//	kineses: kinesis.NewFromConfig(cfg),
	//}
	client := kinesis.NewFromConfig(cfg)
	fmt.Println("NewTestKinesisConsumer", client)

	//_, err = t.CreateStream(context.TODO(), &kinesis.CreateStreamInput{
	//	StreamName: aws.String(streamName),
	//	ShardCount: aws.Int32(1),
	//})
	_, err = client.CreateStream(context.TODO(), &kinesis.CreateStreamInput{
		StreamName: aws.String("ssdasdsa"),
		ShardCount: aws.Int32(1),
	})

	if err != nil {
		// Ignore "ResourceInUseException" as the stream may already exist.
		if _, ok := err.(*types.ResourceInUseException); !ok {
			log.Fatalf("Failed to create stream: %v", err)
		} else {
			fmt.Println("Stream already exists.")
		}
	} else {
		fmt.Println("Stream created.")
	}

	//for _, streamName := range streamNames {
	//	if err = t.createStreamIfNotExists(streamName); err != nil {
	//		return nil, err
	//	}
	//}
	return nil, nil
}

func (t *TestKinesisConsumer) streamExists(streamName string) (bool, error) {
	response, err := t.kineses.ListStreams(context.TODO(), &kinesis.ListStreamsInput{
		Limit: aws.Int32(100),
	})
	if err != nil {
		return false, err
	}
	if len(response.StreamNames) == 0 {
		return false, nil
	}

	for _, streamNameResponse := range response.StreamNames {
		if strings.Contains(streamNameResponse, streamName) {
			return true, nil
		}
	}
	return false, nil
}

func (t *TestKinesisConsumer) createStreamIfNotExists(streamName string) error {

	_, err := t.kineses.CreateStream(context.TODO(), &kinesis.CreateStreamInput{
		StreamName: aws.String("nonlan"),
		ShardCount: aws.Int32(1)})

	if err != nil {
		// Ignore "ResourceInUseException" as the stream may already exist.
		if _, ok := err.(*types.ResourceInUseException); !ok {
			log.Fatalf("Failed to create stream: %v", err)
		} else {
			fmt.Println("Stream already exists.")
		}
	} else {
		fmt.Println("Stream created.")
	}
	return nil
}

func (t *TestKinesisConsumer) FetchFirstStreamMessage(topic string) (types.Record, error) {
	stream := aws.String(topic)
	describeInput := &kinesis.DescribeStreamInput{
		StreamName: stream,
	}

	describeOutput, err := t.kineses.DescribeStream(context.TODO(), describeInput)
	if err != nil {
		return types.Record{}, err
	}
	kinesisStreamName := *describeOutput.StreamDescription.StreamName
	if !strings.EqualFold(kinesisStreamName, topic) {
		return types.Record{}, fmt.Errorf("stream name mismatch. Expected %s, Actual %s", kinesisStreamName, topic)
	}
	if len(describeOutput.StreamDescription.Shards) == 0 {
		return types.Record{}, errors.New("empty shards")
	}

	shardID := describeOutput.StreamDescription.Shards[0].ShardId
	getIteratorInput := &kinesis.GetShardIteratorInput{
		StreamName:        stream,
		ShardId:           shardID,
		ShardIteratorType: types.ShardIteratorType("TRIM_HORIZON"),
	}

	getIteratorOutput, err := t.kineses.GetShardIterator(context.TODO(), getIteratorInput)
	if err != nil {
		return types.Record{}, err
	}

	shardIterator := getIteratorOutput.ShardIterator
	getRecordsInput := &kinesis.GetRecordsInput{
		ShardIterator: shardIterator,
	}

	getRecordsOutput, err := t.kineses.GetRecords(context.TODO(), getRecordsInput)
	if err != nil {
		return types.Record{}, err
	}
	records := getRecordsOutput.Records
	if len(records) == 0 {
		return types.Record{}, errors.New("empty records")
	}

	return records[0], nil
}
