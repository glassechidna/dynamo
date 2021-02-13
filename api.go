package main

import (
	"fmt"
	daxc "github.com/aws/aws-dax-go/dax"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dax"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"strings"
)

type Api struct {
	dynamo *dynamodb.DynamoDB
	dax    *daxc.Dax
}

func (a *Api) ListTablesPages(input *dynamodb.ListTablesInput, cb func(*dynamodb.ListTablesOutput, bool) bool) error {
	return a.dynamo.ListTablesPages(input, cb)
}

func (a *Api) DescribeTable(input *dynamodb.DescribeTableInput) (*dynamodb.DescribeTableOutput, error) {
	return a.dynamo.DescribeTable(input)
}

func (a *Api) QueryPages(input *dynamodb.QueryInput, cb func(*dynamodb.QueryOutput, bool) bool) error {
	if a.dax != nil {
		return a.dax.QueryPages(input, cb)
	}

	return a.dynamo.QueryPages(input, cb)
}

func (a *Api) ScanPages(input *dynamodb.ScanInput, cb func(*dynamodb.ScanOutput, bool) bool) error {
	if a.dax != nil {
		return a.dax.ScanPages(input, cb)
	}

	return a.dynamo.ScanPages(input, cb)
}

func apiClient(daxCluster string, profile string) *Api {
	sess, err := session.NewSessionWithOptions(session.Options{
		Profile:                 profile,
		SharedConfigState:       session.SharedConfigEnable,
		AssumeRoleTokenProvider: stscreds.StdinTokenProvider,
	})
	if err != nil {
		panic(err)
	}

	api := &Api{dynamo: dynamodb.New(sess)}

	if len(daxCluster) == 0 {
		return api
	}

	if !strings.Contains(daxCluster, ".") {
		// must be a cluster name rather than domain name
		dapi := dax.New(sess)
		desc, err := dapi.DescribeClusters(&dax.DescribeClustersInput{ClusterNames: []*string{}})
		if err != nil {
			panic(err)
		}

		if len(desc.Clusters) == 0 {
			panic("no cluster found by that name")
		}

		e := desc.Clusters[0].ClusterDiscoveryEndpoint
		daxCluster = fmt.Sprintf("%s:%d", *e.Address, *e.Port)
	}

	if !strings.Contains(daxCluster, ":") {
		// missing port, assume default
		daxCluster += ":8111"
	}

	cfg := daxc.DefaultConfig()
	cfg.HostPorts = []string{daxCluster}
	cfg.Credentials = sess.Config.Credentials
	cfg.Region = *sess.Config.Region

	api.dax, err = daxc.New(cfg)
	if err != nil {
		panic(err)
	}

	return api
}
