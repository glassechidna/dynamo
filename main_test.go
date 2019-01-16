package main

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestParseSortExpr(t *testing.T) {
	t.Run("less than or equal to", func(t *testing.T) {
		assert.Equal(t, &parsedExpr{
			expression: "#skey <= :skey",
			values:     map[string]string{":skey": "ABCDEF"},
		}, parseSortExpr("<=ABCDEF"))

		assert.Equal(t, &parsedExpr{
			expression: "#skey <= :skey",
			values:     map[string]string{":skey": "ABCDEF"},
		}, parseSortExpr(" < =  ABCDEF "))
	})

	t.Run("greater than or equal to", func(t *testing.T) {
		assert.Equal(t, &parsedExpr{
			expression: "#skey >= :skey",
			values:     map[string]string{":skey": "ABCDEF"},
		}, parseSortExpr(">=ABCDEF"))

		assert.Equal(t, &parsedExpr{
			expression: "#skey >= :skey",
			values:     map[string]string{":skey": "ABCDEF"},
		}, parseSortExpr(" > = ABCDEF "))
	})

	t.Run("less than", func(t *testing.T) {
		assert.Equal(t, &parsedExpr{
			expression: "#skey < :skey",
			values:     map[string]string{":skey": "ABCDEF"},
		}, parseSortExpr("<ABCDEF"))

		assert.Equal(t, &parsedExpr{
			expression: "#skey < :skey",
			values:     map[string]string{":skey": "ABCDEF"},
		}, parseSortExpr(" <  ABCDEF "))
	})

	t.Run("greater than", func(t *testing.T) {
		assert.Equal(t, &parsedExpr{
			expression: "#skey > :skey",
			values:     map[string]string{":skey": "ABCDEF"},
		}, parseSortExpr(">ABCDEF"))

		assert.Equal(t, &parsedExpr{
			expression: "#skey > :skey",
			values:     map[string]string{":skey": "ABCDEF"},
		}, parseSortExpr(" >  ABCDEF "))
	})

	t.Run("explicit equal to", func(t *testing.T) {
		assert.Equal(t, &parsedExpr{
			expression: "#skey = :skey",
			values:     map[string]string{":skey": "ABCDEF"},
		}, parseSortExpr("=ABCDEF"))

		assert.Equal(t, &parsedExpr{
			expression: "#skey = :skey",
			values:     map[string]string{":skey": "ABCDEF"},
		}, parseSortExpr(" =  ABCDEF "))
	})

	t.Run("between", func(t *testing.T) {
		assert.Equal(t, &parsedExpr{
			expression: "#skey between :skey and :skeyb",
			values:     map[string]string{":skey": "ABCDEF", ":skeyb": "XX"},
		}, parseSortExpr("between ABCDEF XX"))

		assert.Equal(t, &parsedExpr{
			expression: "#skey between :skey and :skeyb",
			values:     map[string]string{":skey": "ABCDEF", ":skeyb": "XX11"},
		}, parseSortExpr(" between  ABCDEF XX11 "))
	})

	t.Run("begins with", func(t *testing.T) {
		assert.Equal(t, &parsedExpr{
			expression: "begins_with(#skey, :skey)",
			values:     map[string]string{":skey": "ABCDEF"},
		}, parseSortExpr("begins_with(ABCDEF)"))

		assert.Equal(t, &parsedExpr{
			expression: "begins_with(#skey, :skey)",
			values:     map[string]string{":skey": "ABCDEF"},
		}, parseSortExpr("begins_with  (ABCDEF )"))

		assert.Equal(t, &parsedExpr{
			expression: "begins_with(#skey, :skey)",
			values:     map[string]string{":skey": "ABCDEF"},
		}, parseSortExpr(" begins_with  ABCDEF "))
	})

	t.Run("asterisk form of begins with", func(t *testing.T) {
		assert.Equal(t, &parsedExpr{
			expression: "begins_with(#skey, :skey)",
			values:     map[string]string{":skey": "ABCDEF"},
		}, parseSortExpr("ABCDEF*"))

		assert.Equal(t, &parsedExpr{
			expression: "begins_with(#skey, :skey)",
			values:     map[string]string{":skey": "ABCDEF"},
		}, parseSortExpr("  ABCDEF*"))

		assert.Equal(t, &parsedExpr{
			expression: "begins_with(#skey, :skey)",
			values:     map[string]string{":skey": "ABCDEF "},
		}, parseSortExpr("  ABCDEF *"))
	})

	t.Run("implict equal to", func(t *testing.T) {
		assert.Equal(t, &parsedExpr{
			expression: "#skey = :skey",
			values:     map[string]string{":skey": "ABCDEF"},
		}, parseSortExpr("ABCDEF"))

		assert.Equal(t, &parsedExpr{
			expression: "#skey = :skey",
			values:     map[string]string{":skey": "ABCDEF"},
		}, parseSortExpr("   ABCDEF "))
	})
}
