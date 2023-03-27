package sheet

import (
	"context"

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
