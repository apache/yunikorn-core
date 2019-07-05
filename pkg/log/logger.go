package log

import (
	"go.uber.org/zap"
)


var Logger *zap.Logger

func init() {
	if Logger = zap.L(); Logger == nil {
		panic("unexpected logging!!!")
		Logger, _= zap.NewDevelopment()
	}

	Logger.Info("#############################")
	Logger.Info("#############################")
	Logger.Info("#############################")
	Logger.Info("#############################")
}