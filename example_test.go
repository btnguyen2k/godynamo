package godynamo_test

import (
	"context"
	"database/sql"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/btnguyen2k/godynamo"
	"log"
)

func ExampleRegisterAWSConfig() {
	awsConfig, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	godynamo.RegisterAWSConfig(awsConfig)
	db, err := sql.Open("godynamo", "Region=<region>;AkId=<aws-key-id>;Secret_Key=<aws-secret-key>")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
}

func ExampleDeregisterAWSConfig() {
	awsConfig, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	godynamo.RegisterAWSConfig(awsConfig)
	db, err := sql.Open("godynamo", "Region=<region>;AkId=<aws-key-id>;Secret_Key=<aws-secret-key>")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// works the same as it did before registration.
	godynamo.DeregisterAWSConfig()
	db2, err := sql.Open("godynamo", "Region=<region>;AkId=<aws-key-id>;Secret_Key=<aws-secret-key>")
	if err != nil {
		log.Fatal(err)
	}
	defer db2.Close()
}
