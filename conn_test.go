package godynamo

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func TestDInpput(t *testing.T) {
	testCases := []struct {
		name     string
		query    string
		params   []types.AttributeValue
		limit    *int32
		expected *dynamodb.ExecuteStatementInput
	}{
		{
			name:   "no params",
			query:  "SELECT * FROM test",
			params: []types.AttributeValue{},
			limit:  nil,
			expected: &dynamodb.ExecuteStatementInput{
				Statement:              aws.String("SELECT * FROM test"),
				ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
			},
		},
		{
			name:  "with params",
			query: "SELECT * FROM test WHERE id = ?",
			params: []types.AttributeValue{
				&types.AttributeValueMemberS{Value: "test"},
			},
			limit: nil,
			expected: &dynamodb.ExecuteStatementInput{
				Statement:              aws.String("SELECT * FROM test WHERE id = ?"),
				Parameters:             []types.AttributeValue{&types.AttributeValueMemberS{Value: "test"}},
				ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := dInput(tc.query, tc.params, tc.limit)
			if actual.Statement == nil {
				t.Errorf("expected statement to be set")
			}
			if *actual.Statement != *tc.expected.Statement {
				t.Errorf("expected statement to be %s, got %s", *tc.expected.Statement, *actual.Statement)
			}
			if actual.ReturnConsumedCapacity != tc.expected.ReturnConsumedCapacity {
				t.Errorf("expected ReturnConsumedCapacity to be %s, got %s", tc.expected.ReturnConsumedCapacity, actual.ReturnConsumedCapacity)
			}
		})
	}

}
