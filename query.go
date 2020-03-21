package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/pkg/errors"
	"regexp"
	"strings"
)

func queryForArgs(api *Api, args []string) (*dynamodb.QueryInput, error) {
	table := args[0]
	tableDescription, err := tableDescription(api, table)
	if err != nil {
		return nil, err
	}

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

func tableDescription(api *Api, table string) (*dynamodb.TableDescription, error) {
	describeResp, err := api.DescribeTable(&dynamodb.DescribeTableInput{TableName: &table})
	if err != nil {
		return nil, errors.WithStack(err)
	}

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
}
