package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	bqutil "github.com/danielchristian-tokped/bigquery-cluster/bigquery"
	"github.com/danielchristian-tokped/bigquery-cluster/constant"
	sheetutil "github.com/danielchristian-tokped/bigquery-cluster/sheet"
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

var spreadsheetID = "1ROdcDV71who85wabn5fIV6K8ZjdinbhBOqyWzI25GjI"
var sheetrange = "B29:H29"

var MapTableIDCluster = map[string][]string{}
var ProjectIDCache = map[string]*bigquery.Client{}
var BQModule *bqutil.BigQueryModule

func main() {

	clusterEnv := getEnvironment()
	initiateLog(clusterEnv)

	var err error

	var prevProjectID string
	ctx := context.Background()

	googleAuthCfg := GoogleOAuth{}
	getConfigFile(&googleAuthCfg, clusterEnv)

	serviceAccount, err := json.Marshal(googleAuthCfg)
	if err != nil {
		fmt.Println("Service account marshall error:", err)
		log.Errorf("Service account marshall error:", err)
		return
	}

	// Get Project ID, Dataset ID, TableID from Sheet
	SheetModule := sheetutil.New(ctx, &sheetutil.SheetModule{
		SheetAuth: serviceAccount,
	})

	rangeData := fmt.Sprintf("%s!%s", constant.WorksheetID, sheetrange)
	resp, err := SheetModule.RetrieveDataFromSheet(ctx, spreadsheetID, rangeData)
	if err != nil {
		log.Printf("Unable to retrieve data from sheet: %v\n", err)
		log.Errorf("Unable to retrieve data from sheet: %v\n", err)
		return
	}

	for id, row := range resp.Values {
		clustered, _ := strconv.ParseBool(row[6].(string))
		if clustered {
			continue
		}

		tableIdentifier := fmt.Sprintf("%s. %s", strconv.Itoa(id), row[0])

		MapTableIDCluster[tableIdentifier] = make([]string, 0, 4)

		for colNo := 1; colNo < 5; colNo++ {
			if row[colNo].(string) != constant.NONE {
				MapTableIDCluster[tableIdentifier] = append(MapTableIDCluster[tableIdentifier], row[colNo].(string))
			}
		}
	}

	// If our range is B12:C28, get only the 12
	startRow, _ := strconv.Atoi(strings.Split(sheetrange, ":")[0][1:])

	for tableIdentifier, clusterColumn := range MapTableIDCluster {

		identifier, tableName := bqutil.SplitTableIdentifier(tableIdentifier)
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

		var errMessage string
		err = BQModule.CreateTableClustered(ctx, datasetID, tableID, clusterColumn)
		if err != nil {
			errMessage = err.Error()

			fmt.Printf("Create Table Clustered Error on %s.%s.%s:\n%#v\n", projectID, datasetID, tableID, err)
			log.Errorf("Create Table Clustered Error on %s.%s.%s:\n%#v\n", projectID, datasetID, tableID, err)
		} else {
			timeEnd := time.Now()
			timeExecution := time.Since(timeStart)

			log.Infof("%s.%s.%s column %v has been clustered at %s. Clustering was done for %dms\n", projectID, datasetID, tableID, strings.Join(clusterColumn[:], ", "), timeEnd, timeExecution)
			fmt.Printf("%s.%s.%s column %v has been clustered at %s. Clustering was done for %dms\n", projectID, datasetID, tableID, strings.Join(clusterColumn[:], ", "), timeEnd, timeExecution)
		}

		currentRow := strconv.Itoa(startRow + identifier)

		_, err := SheetModule.UpdateCellValue(ctx, spreadsheetID, currentRow, errMessage)
		if err != nil {
			fmt.Printf("Error Updating on row %s:\n%#v\n", currentRow, err)
			log.Errorf("Error Updating on row %s:\n%#v\n", currentRow, err)
		}

		prevProjectID = projectID

	}

	fmt.Println("Process DONE.")

}

func initiateLog(clusterEnv string) {
	var fatalLogFile, errorLogFile, infoLogFile string

	fatalLogFile = fmt.Sprintf("%s.%s.%s", constant.LogPath, clusterEnv, constant.FatalLogName)
	errorLogFile = fmt.Sprintf("%s.%s.%s", constant.LogPath, clusterEnv, constant.ErrorLogName)
	infoLogFile = fmt.Sprintf("%s.%s.%s", constant.LogPath, clusterEnv, constant.InfoLogName)

	errFatalLogger, err := log.NewLogger(log.Zerolog, &logger.Config{
		AppName:  "Simple App BigQuery Clustering",
		Level:    log.FatalLevel, // please ignore
		LogFile:  fatalLogFile,
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
		LogFile:  errorLogFile,
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
		LogFile:  infoLogFile,
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

func getConfigFile(config *GoogleOAuth, clusterEnv string) error {

	rootpath, _ := os.Getwd()

	fmt.Println("Cluster Environment:", clusterEnv)
	filepath := rootpath + constant.CredentialPath
	filepath = filepath + constant.PrefixFileSA + "." + clusterEnv + ".json"

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

func getEnvironment() (clusterEnv string) {
	clusterEnv = os.Getenv(constant.CLUSTER_ENV)
	if clusterEnv == "" {
		clusterEnv = constant.STAGING
	}

	return clusterEnv
}
