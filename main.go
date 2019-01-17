package main

import (
	"encoding/json"
	"fmt"
	"github.com/TylerBrock/colorjson"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/davecgh/go-spew/spew"
	"github.com/mattn/go-colorable"
	"github.com/mattn/go-isatty"
	"github.com/spf13/pflag"
	"io"
	"os"
	"regexp"
	"strings"
)

var maxCount int

type Dynamo struct {
	api     dynamodbiface.DynamoDBAPI
	w       io.Writer
	emitted int
}

func main() {
	/*
		len(os.Args)

		1 => just app name, do help
		2 => app and table, do a scan
		3 => app, table, pkey => do a query
		4 => app, table, pkey, skey => implies equality if no operator in skey
	*/

	pflag.IntVarP(&maxCount, "number", "n", 10, "maximum number of items to output. 0 for no limit")
	pflag.Parse()
	args := pflag.Args()

	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))
	api := dynamodb.New(sess)

	var w io.Writer = os.Stdout
	if isatty.IsTerminal(os.Stdout.Fd()) {
		w = colorable.NewColorable(os.Stdout)
	}

	d := &Dynamo{
		api: api,
		w:   w,
	}
	d.Run(args)
}

func (d *Dynamo) Run(args []string) {
	if len(args) == 1 { // only table name passed in
		input := &dynamodb.ScanInput{
			TableName: aws.String(args[0]),
			Limit:     aws.Int64(100),
		}
		err := d.api.ScanPages(input, func(page *dynamodb.ScanOutput, lastPage bool) bool {
			return d.write(convert(page.Items)) || lastPage
		})
		if err != nil {
			spew.Dump(err)
		}
	} else {
		input, _ := queryForArgs(d.api, args)

		err := d.api.QueryPages(input, func(page *dynamodb.QueryOutput, lastPage bool) bool {
			return d.write(convert(page.Items)) || lastPage
		})
		if err != nil {
			spew.Dump(err)
		}
	}
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

func queryForArgs(api dynamodbiface.DynamoDBAPI, args []string) (*dynamodb.QueryInput, error) {
	table := args[0]
	tableDescription, _ := tableDescription(api, table)

	attrType := func(name string) string {
		for _, def := range tableDescription.AttributeDefinitions {
			if name == *def.AttributeName {
				return *def.AttributeType
			}
		}
		panic(fmt.Sprintf("unknown key: %s", name))
	}

	partitionKeyValue := args[1]
	partitionKeyName := *tableDescription.KeySchema[0].AttributeName

	expression := ""
	names := map[string]*string{}
	values := map[string]*dynamodb.AttributeValue{}

	setValue := func(values map[string]*dynamodb.AttributeValue, name, key, value string) {
		typ := attrType(name)
		switch typ {
		case dynamodb.ScalarAttributeTypeS:
			values[key] = &dynamodb.AttributeValue{S: &value}
		case dynamodb.ScalarAttributeTypeB:
			values[key] = &dynamodb.AttributeValue{B: []byte(value)}
		case dynamodb.ScalarAttributeTypeN:
			values[key] = &dynamodb.AttributeValue{N: &value}
		}
	}

	setValue(values, partitionKeyName, ":partitionKey", partitionKeyValue)

	if len(args) == 2 { // table, partition value
		expression = "#partitionKey = :partitionKey"
		names = map[string]*string{
			"#partitionKey": &partitionKeyName,
		}
	} else if len(args) == 3 { // table, partition value, sort (value|expression)
		sortKeyName := *tableDescription.KeySchema[1].AttributeName
		expr := parseSortExpr(args[2])
		expression = fmt.Sprintf("#partitionKey = :partitionKey and %s", expr.expression)
		for k, v := range expr.values {
			setValue(values, sortKeyName, k, v)
		}
		names = map[string]*string{
			"#partitionKey": &partitionKeyName,
			"#skey":         &sortKeyName,
		}
	}

	input := &dynamodb.QueryInput{
		TableName:                 &table,
		KeyConditionExpression:    &expression,
		ExpressionAttributeValues: values,
		ExpressionAttributeNames:  names,
	}

	return input, nil
}

func tableDescription(api dynamodbiface.DynamoDBAPI, table string) (*dynamodb.TableDescription, error) {
	describeResp, _ := api.DescribeTable(&dynamodb.DescribeTableInput{TableName: &table})
	tableDescription := describeResp.Table
	return tableDescription, nil
}

type parsedExpr struct {
	expression string
	values     map[string]string
}

func parseSortExpr(input string) *parsedExpr {
	exprs := []struct {
		re   *regexp.Regexp
		expr string
	}{
		{re: regexp.MustCompile(`^\s*<\s*=\s*(\S+)`), expr: "#skey <= :skey"},
		{re: regexp.MustCompile(`^\s*>\s*=\s*(\S+)`), expr: "#skey >= :skey"},
		{re: regexp.MustCompile(`^\s*<\s*(\S+)`), expr: "#skey < :skey"},
		{re: regexp.MustCompile(`^\s*>\s*(\S+)`), expr: "#skey > :skey"},
		{re: regexp.MustCompile(`^\s*=\s*(\S+)`), expr: "#skey = :skey"},
		{re: regexp.MustCompile(`begins_with\s*\(?\s*([^)\s]+)\s*\)?`), expr: "begins_with(#skey, :skey)"},
		{re: regexp.MustCompile(`\s*([^*]+)\*`), expr: "begins_with(#skey, :skey)"},
	}

	for _, expr := range exprs {
		if m := expr.re.FindStringSubmatch(input); len(m) > 0 {
			return &parsedExpr{
				expression: expr.expr,
				values:     map[string]string{":skey": m[1]},
			}
		}
	}

	between := regexp.MustCompile(`\s*between\s+(\S+)\s+(\S+)`)
	if m := between.FindStringSubmatch(input); len(m) > 0 {
		return &parsedExpr{
			expression: "#skey between :skey and :skeyb",
			values: map[string]string{
				":skey":  m[1],
				":skeyb": m[2],
			},
		}
	}

	return &parsedExpr{
		expression: "#skey = :skey",
		values:     map[string]string{":skey": strings.TrimSpace(input)},
	}

	return nil
}
