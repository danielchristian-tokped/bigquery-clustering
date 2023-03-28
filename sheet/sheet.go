package sheet

import (
	"context"
	"fmt"
	"time"

	"github.com/danielchristian-tokped/bigquery-cluster/constant"
	"google.golang.org/api/option"
	sheets "google.golang.org/api/sheets/v4"
)

type SheetModule struct {
	SheetService *sheets.Service
	SheetAuth    []byte
}

func New(ctx context.Context, module *SheetModule) *SheetModule {
	sheetService, _ := sheets.NewService(ctx, option.WithCredentialsJSON(module.SheetAuth))
	module.SheetService = sheetService

	return module
}

func (sm *SheetModule) RetrieveDataFromSheet(ctx context.Context, spreadsheetID, rangeData string) (resp *sheets.ValueRange, err error) {
	resp, err = sm.SheetService.Spreadsheets.Values.Get(spreadsheetID, rangeData).Do()
	if err != nil {
		return nil, err
	}

	if len(resp.Values) == 0 {
		fmt.Println("No data found.")
	}

	return resp, nil
}

func (sm *SheetModule) UpdateCellValue(ctx context.Context, spreadsheetID, rowNo, errMessage string) (resp *sheets.UpdateValuesResponse, err error) {
	cellNo := fmt.Sprintf("%s!H%s", constant.WorksheetID, rowNo)

	updateValue := []interface{}{false, time.Now(), errMessage}
	if errMessage == "" {
		updateValue[0] = true
	}

	valuerange := &sheets.ValueRange{
		Values: [][]interface{}{
			updateValue,
		},
	}
	resp, err = sm.SheetService.Spreadsheets.Values.Update(spreadsheetID, cellNo, valuerange).ValueInputOption("RAW").Context(ctx).Do()
	if err != nil {
		return nil, err
	}

	return resp, nil
}
