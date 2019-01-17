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
	"github.com/mattn/go-isatty"
	"github.com/spf13/pflag"
	"os"
	"regexp"
	"strings"
)

var maxCount int
var jsonLines bool

func main() {
	/*
		len(os.Args)

		1 => just app name, do help
		2 => app and table, do a scan
		3 => app, table, pkey => do a query
		4 => app, table, pkey, skey => implies equality if no operator in skey
	*/

	pflag.IntVarP(&maxCount, "number", "n", 10, "maximum number of items to output. 0 for no limit")
	pflag.BoolVarP(&jsonLines, "jsonlines", "l", false, "emit output as an item per line (not in an array)")
	pflag.Parse()
	args := pflag.Args()

	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))
	api := dynamodb.New(sess)

	jsonItems := []interface{}{}

	if len(args) == 1 { // only table name passed in
		input := &dynamodb.ScanInput{
			TableName: aws.String(args[0]),
			Limit:     aws.Int64(int64(maxCount)),
		}
		err := api.ScanPages(input, func(page *dynamodb.ScanOutput, lastPage bool) bool {
			return appendItems(page.Items, &jsonItems) || lastPage
		})
		if err != nil {
			spew.Dump(err)
		}
	} else {
		input, _ := queryForArgs(api, args)
		input.Limit = aws.Int64(int64(maxCount))

		err := api.QueryPages(input, func(page *dynamodb.QueryOutput, lastPage bool) bool {
			return appendItems(page.Items, &jsonItems) || lastPage
		})
		if err != nil {
			spew.Dump(err)
		}
	}

	emit(jsonItems)
}

func emit(jsonItems []interface{}) {
	var marshaller = json.Marshal
	if isatty.IsTerminal(os.Stdout.Fd()) {
		f := colorjson.NewFormatter()
		if !jsonLines {
			f.Indent = 2
		}
		marshaller = f.Marshal
	}

	if jsonLines {
		for _, item := range jsonItems {
			bytes, _ := marshaller(item)
			fmt.Println(string(bytes))
		}
	} else {
		bytes, _ := marshaller(jsonItems)
		fmt.Println(string(bytes))
	}
}

func appendItems(items []map[string]*dynamodb.AttributeValue, jsonItems *[]interface{}) bool {
	take := len(items)
	if take > maxCount - len(*jsonItems) {
		take = maxCount - len(*jsonItems)
	}

	for _, item := range items {
		jsonItem := itemToJsonable(item)
		*jsonItems = append(*jsonItems, jsonItem)
	}

	return len(*jsonItems) < maxCount
}

func queryForArgs(api dynamodbiface.DynamoDBAPI, args []string) (*dynamodb.QueryInput, error) {
	table := args[0]
	tableDescription, _ := tableDescription(api, table)

	partitionKeyValue := args[1]
	partitionKeyName := *tableDescription.KeySchema[0].AttributeName

	expression := ""
	names := map[string]*string{}
	values := map[string]*dynamodb.AttributeValue{
		":partitionKey": {S: &partitionKeyValue},
	}

	if len(args) == 2 { // table, partition value
		expression = "#partitionKey = :partitionKey"
		values = map[string]*dynamodb.AttributeValue{
			":partitionKey": {S: &partitionKeyValue},
		}
		names = map[string]*string{
			"#partitionKey": &partitionKeyName,
		}
	} else if len(args) == 3 { // table, partition value, sort (value|expression)
		expr := parseSortExpr(args[2])
		expression = fmt.Sprintf("#partitionKey = :partitionKey and %s", expr.expression)
		for k, v := range expr.values {
			v := v
			values[k] = &dynamodb.AttributeValue{S: &v}
		}
		names = map[string]*string{
			"#partitionKey": &partitionKeyName,
			"#skey":         tableDescription.KeySchema[1].AttributeName,
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

func itemToJsonable(item map[string]*dynamodb.AttributeValue) map[string]interface{} {
	ret := map[string]interface{}{}
	err := dynamodbattribute.UnmarshalMap(item, &ret)
	if err != nil {
		panic(err)
	}
	return ret
}
