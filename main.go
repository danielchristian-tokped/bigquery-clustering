package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	bqutil "github.com/danielchristian-tokped/bigquery-cluster/bigquery"
	"github.com/tokopedia/tdk/go/log"
	"github.com/tokopedia/tdk/go/log/logger"
)

type GoogleOAuth struct {
	Type                    string `json:"type"`
	ProjectID               string `json:"project_id"`
	PrivateKeyID            string `json:"private_key_id"`
	PrivateKey              string `json:"private_key"`
	ClientEmail             string `json:"client_email"`
	ClientID                string `json:"client_id"`
	AuthURI                 string `json:"auth_uri"`
	TokenURI                string `json:"token_uri"`
	AuthProviderX509CertURL string `json:"auth_provider_x509_cert_url"`
	ClientX509CertURL       string `json:"client_x509_cert_url"`
}

const CLUSTER_ENV = "CLUSTERENV"
const credentialPath = "/credentials/"
const PrefixFileSA = "clustering-service-account"

var spreadsheetID = "1ROdcDV71who85wabn5fIV6K8ZjdinbhBOqyWzI25GjI"
var worksheet = "Sheet1"
var sheetrange = ""

var MapTableIDCluster = map[string][]string{}
var ProjectIDCache = map[string]*bigquery.Client{}
var BQModule *bqutil.BigQueryModule

func main() {

	initiateLog()

	var err error

	var prevProjectID string
	ctx := context.Background()

	googleAuthCfg := GoogleOAuth{}
	getConfigFile(&googleAuthCfg)

	serviceAccount, err := json.Marshal(googleAuthCfg)
	if err != nil {
		fmt.Println("Service account marshall error:", err)
		log.Errorf("Service account marshall error:", err)
	}

	// Get Project ID, Dataset ID, TableID from Sheet

	for tableName, clusterColumn := range MapTableIDCluster {

		projectID, datasetID, tableID := bqutil.SplitTableName(tableName)

		if bqClient, cached := ProjectIDCache[projectID]; !cached {

			//If amount of BQ Client is 5 or more, close previous BQ client and delete from map
			if len(ProjectIDCache) > 5 {
				prevBQClient := ProjectIDCache[prevProjectID]
				prevBQClient.Close()

				delete(ProjectIDCache, prevProjectID)
			}

			//Initiate New BQ Client
			BQModule = bqutil.New(ctx, projectID, &bqutil.BigQueryModule{
				BQAuth: serviceAccount,
			})

			ProjectIDCache[projectID] = BQModule.Client

		} else {
			BQModule.Client = bqClient
		}

		timeStart := time.Now()

		err = BQModule.CreateTableClustered(ctx, datasetID, tableID, clusterColumn)
		if err != nil {
			fmt.Printf("Create Table Clustered Error on %s.%s.%s:\n%#v\n", projectID, datasetID, tableID, err)
			log.Errorf("Create Table Clustered Error on %s.%s.%s:\n%#v\n", projectID, datasetID, tableID, err)
		} else {
			timeEnd := time.Now()
			timeExecution := time.Since(timeStart)

			log.Infof("%s.%s.%s column %v has been clustered at %s. Clustering was done for %dms\n", projectID, datasetID, tableID, strings.Join(clusterColumn[:], ", "), timeEnd, timeExecution)
			fmt.Printf("%s.%s.%s column %v has been clustered at %s. Clustering was done for %dms\n", projectID, datasetID, tableID, strings.Join(clusterColumn[:], ", "), timeEnd, timeExecution)
		}

		if prevProjectID != projectID {
			prevProjectID = projectID
		}

	}

	fmt.Println("Process DONE.")

}

func initiateLog() {
	errFatalLogger, err := log.NewLogger(log.Zerolog, &logger.Config{
		AppName:  "Simple App BigQuery Clustering",
		Level:    log.FatalLevel, // please ignore
		LogFile:  "/var/log/tokopedia/bigquery-cluster/bigquery-cluster.error-fatal.log",
		Caller:   true,
		UseColor: true,
		UseJSON:  true,
	})
	if err != nil {
		log.Fatal(err)
	}
	err = log.SetLogger(log.FatalLevel, errFatalLogger)
	if err != nil {
		log.Fatal(err)
	}

	log.ErrorWithFields("this is a log fatal error", map[string]interface{}{
		"req_id":   "bpbq9t3o6hiu1038scmg",                 // a request id, must be injected from handler/middleware in first time request is coming. will be passed in each process.
		"trace-id": "92667eac-467e-45eb-8f66-6f6cf6c961dd", // supporting - guid - for jump to code (optional)
		"ctx_id":   "10",                                   // Any id, can be transaction id, invoice id, user id. If no id
		"err":      "err.Error()",                          // Error stack trace.
		"metadata": map[string]interface{}{ // additional data that you want to add to make your log more clear.
			"raw-message": "test error",
			"context":     "init log",
		},
	})

	errorLogger, err := log.NewLogger(log.Zerolog, &logger.Config{
		AppName:  "Simple App BigQuery Clustering",
		Level:    log.ErrorLevel, // please ignore
		LogFile:  "/var/log/tokopedia/bigquery-cluster/bigquery-cluster.error.log",
		Caller:   true,
		UseColor: true,
		UseJSON:  true,
	})
	if err != nil {
		log.Fatal(err)
	}
	err = log.SetLogger(log.ErrorLevel, errorLogger)
	if err != nil {
		log.Fatal(err)
	}

	log.ErrorWithFields("this is a log error", map[string]interface{}{
		"req_id":   "bpbq9t3o6hiu1038scmg",                 // a request id, must be injected from handler/middleware in first time request is coming. will be passed in each process.
		"trace-id": "92667eac-467e-45eb-8f66-6f6cf6c961dd", // supporting - guid - for jump to code (optional)
		"ctx_id":   "10",                                   // Any id, can be transaction id, invoice id, user id. If no id
		"err":      "err.Error()",                          // Error stack trace.
		"metadata": map[string]interface{}{ // additional data that you want to add to make your log more clear.
			"raw-message": "test error",
			"context":     "init log",
		},
	})

	infoLogger, err := log.NewLogger(log.Zerolog, &logger.Config{
		AppName:  "Simple App BigQuery Clustering",
		Level:    log.InfoLevel, // please ignore
		LogFile:  "/var/log/tokopedia/bigquery-cluster/bigquery-cluster.info.log",
		Caller:   true,
		UseColor: true,
		UseJSON:  true,
	})
	if err != nil {
		log.Fatal(err)
	}
	err = log.SetLogger(log.InfoLevel, infoLogger)
	if err != nil {
		log.Fatal(err)
	}
	log.InfoWithFields("this is a log info", map[string]interface{}{
		"trace-id": "b7a1b1a3-c16e-482e-82b3-3c1e15b6b35c",
		"data": map[string]interface{}{
			"raw-message": "test info",
			"context":     "init log",
		},
	})
}

func getConfigFile(config *GoogleOAuth) error {

	rootpath, _ := os.Getwd()

	clusterEnv := os.Getenv(CLUSTER_ENV)
	if clusterEnv == "" {
		clusterEnv = "staging"
	}

	fmt.Println("Cluster Environment:", clusterEnv)
	filepath := rootpath + credentialPath
	filepath = filepath + PrefixFileSA + "." + clusterEnv + ".json"

	fileStream, err := os.Open(filepath)
	if err != nil {
		log.Fatalln("File path not found:", err)
		return err
	}

	fileByte, err := ioutil.ReadAll(fileStream)
	if err != nil {
		log.Fatalln("File stream error:", err)
		return err
	}

	err = json.Unmarshal(fileByte, config)
	if err != nil {
		log.Fatalln("Unmarshal error:", err)
		return err
	}

	return nil
}
