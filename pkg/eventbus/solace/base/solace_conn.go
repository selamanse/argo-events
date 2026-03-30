package base

import "go.uber.org/zap"

type SolaceConnection struct {
	Logger *zap.SugaredLogger
}

func NewSolaceConnection(logger *zap.SugaredLogger) *SolaceConnection {
	return &SolaceConnection{
		Logger: logger,
	}
}
