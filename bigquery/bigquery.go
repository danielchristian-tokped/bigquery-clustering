package bigquery

import (
	"context"
	"strconv"
	"strings"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/option"
)

type BigQueryModule struct {
	Client *bigquery.Client
	BQAuth []byte
}

func New(ctx context.Context, projectID string, module *BigQueryModule) *BigQueryModule {
	client, _ := bigquery.NewClient(ctx, projectID, option.WithCredentialsJSON(module.BQAuth))
	module.Client = client

	return module
}

// createTableClustered demonstrates creating a BigQuery table with advanced properties like
// partitioning and clustering features.
func (bq *BigQueryModule) CreateTableClustered(ctx context.Context, datasetID, tableID string, clusterColumn []string) error {

	metaData := bigquery.TableMetadataToUpdate{
		Clustering: &bigquery.Clustering{
			Fields: clusterColumn,
		},
	}

	tableRef := bq.Client.Dataset(datasetID).Table(tableID)
	md, err := tableRef.Metadata(ctx)
	if err != nil {
		return err
	}
	if _, err := tableRef.Update(ctx, metaData, md.ETag); err != nil {
		return err
	}
	return nil
}

func SplitTableIdentifier(tableIdentifier string) (identifier int, tableName string) {
	identifierSplit := strings.Split(tableIdentifier, ". ")
	identifier, _ = strconv.Atoi(identifierSplit[0])

	return identifier, identifierSplit[1]
}

func SplitTableName(tableName string) (projectID, datasetID, tableID string) {
	tableSplit := strings.Split(tableName, ".")

	return tableSplit[0], tableSplit[1], tableSplit[2]
}
