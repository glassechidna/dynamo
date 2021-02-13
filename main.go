package main

import (
	"encoding/json"
	"fmt"
	"github.com/TylerBrock/colorjson"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/davecgh/go-spew/spew"
	"github.com/mattn/go-colorable"
	"github.com/mattn/go-isatty"
	"github.com/spf13/pflag"
	"io"
	"os"
	"sort"
	"strings"
)

var maxCount int
var daxCluster string
var profile string

type Dynamo struct {
	api     *Api
	w       io.Writer
	emitted int
}

func main() {
	/*
		len(os.Args)

		1 => just app name, list tables
		2 => app and table, do a scan
		3 => app, table, pkey => do a query
		4 => app, table, pkey, skey => implies equality if no operator in skey
	*/

	pflag.IntVarP(&maxCount, "number", "n", 10, "maximum number of items to output. 0 for no limit")
	pflag.StringVar(&daxCluster, "dax", "", "Address of DAX cluster")
	pflag.StringVar(&profile, "profile", "", "~/.aws/config profile to use")
	pflag.Parse()
	args := pflag.Args()

	var w io.Writer = os.Stdout
	if isatty.IsTerminal(os.Stdout.Fd()) {
		w = colorable.NewColorable(os.Stdout)
	}

	d := &Dynamo{
		api: apiClient(daxCluster, profile),
		w:   w,
	}
	d.Run(args)
}

func (d *Dynamo) Run(args []string) {
	var err error
	switch len(args) {
	case 0:
		err = d.tables()
	case 1:
		err = d.scan(args[0])
	default:
		err = d.query(args)
	}

	if err != nil {
		spew.Dump(err)
		os.Exit(1)
	}
}

func (d *Dynamo) tables() error {
	names := []string{}
	err := d.api.ListTablesPages(&dynamodb.ListTablesInput{}, func(page *dynamodb.ListTablesOutput, lastPage bool) bool {
		for _, name := range page.TableNames {
			names = append(names, *name)
		}
		return !lastPage
	})

	sort.Slice(names, func(i, j int) bool {
		return strings.ToLower(names[i]) < strings.ToLower(names[j])
	})

	fmt.Println(strings.Join(names, "\n"))
	return err
}

func (d *Dynamo) query(args []string) error {
	input, err := queryForArgs(d.api, args)
	if err != nil {
		return err
	}

	return d.api.QueryPages(input, func(page *dynamodb.QueryOutput, lastPage bool) bool {
		return d.write(convert(page.Items)) || lastPage
	})
}

func (d *Dynamo) scan(table string) error {
	input := &dynamodb.ScanInput{
		TableName: aws.String(table),
		Limit:     aws.Int64(100),
	}

	return d.api.ScanPages(input, func(page *dynamodb.ScanOutput, lastPage bool) bool {
		return d.write(convert(page.Items)) || lastPage
	})
}

func convert(items []map[string]*dynamodb.AttributeValue) []interface{} {
	ret := []interface{}{}
	err := dynamodbattribute.UnmarshalListOfMaps(items, &ret)
	if err != nil {
		panic(err)
	}
	return ret
}

func (d *Dynamo) write(jsonItems []interface{}) bool {
	var marshaller = json.Marshal
	if isatty.IsTerminal(os.Stdout.Fd()) {
		f := colorjson.NewFormatter()
		f.Indent = 2
		marshaller = f.Marshal
	}

	for _, item := range jsonItems {
		bytes, _ := marshaller(item)
		_, err := fmt.Fprintln(d.w, string(bytes))
		if err != nil {
			if err == io.ErrClosedPipe {
				return false
			}
			spew.Fdump(os.Stderr, err)
			panic(err)
		}
		d.emitted++
		if maxCount > 0 && d.emitted >= maxCount {
			return false
		}
	}

	return true
}
