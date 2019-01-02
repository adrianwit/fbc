package fbc

import (
	"context"
	"firebase.google.com/go"
	"firebase.google.com/go/db"
	"fmt"
	"github.com/pkg/errors"
	"github.com/viant/dsc"
	"github.com/viant/toolbox"
	"google.golang.org/api/option"
)

const (
	storageBucketKey = "storageBucket"
	projectIDKey     = "projectID"
	databaseURLKey   = "databaseURL"
)

//AppPointer represents an app pointer context key
var AppPointerKey = (*firebase.App)(nil)

//ClientPointerKey represents an client pointer key
var ClientPointerKey = (*db.Client)(nil)

//ClientPointerKey represents an context pointer key
var ContextPointerKey = (*context.Context)(nil)

func asClient(connection dsc.Connection) (*db.Client, context.Context, error) {
	client := connection.Unwrap(ClientPointerKey).(*db.Client)
	ctx := connection.Unwrap(ContextPointerKey).(*context.Context)
	return client, *ctx, nil
}

func asApp(connection dsc.Connection) (*firebase.App, context.Context, error) {
	client := connection.Unwrap(AppPointerKey).(*firebase.App)
	ctx := connection.Unwrap(ContextPointerKey).(*context.Context)
	return client, *ctx, nil
}

type connection struct {
	*dsc.AbstractConnection
	app       *firebase.App
	client    *db.Client
	ctx       *context.Context
	cancelCtx context.CancelFunc
	dbName    string
}

func (c *connection) CloseNow() error {
	c.cancelCtx()
	return nil
}

func (c *connection) Unwrap(targetType interface{}) interface{} {
	if targetType == AppPointerKey {
		return c.app
	} else if targetType == ClientPointerKey {
		return c.client
	} else if targetType == ContextPointerKey {
		return c.ctx
	}
	panic(fmt.Sprintf("unsupported targetType type %v", targetType))
}

type connectionProvider struct {
	*dsc.AbstractConnectionProvider
}

func (p *connectionProvider) NewConnection() (dsc.Connection, error) {
	config := p.ConnectionProvider.Config()

	firebaseConfig := &firebase.Config{
		DatabaseURL:   config.Get(databaseURLKey),
		ProjectID:     config.Get(projectIDKey),
		StorageBucket: config.Get(storageBucketKey),
	}
	if firebaseConfig.DatabaseURL == "" {
		return nil, errors.New("databaseURL was empty")
	}
	if firebaseConfig.ProjectID == "" {
		return nil, errors.New("projectID was empty")
	}
	//if firebaseConfig.StorageBucket == "" {
	//	return nil, errors.New("storageBucket was empty")
	//}

	var credentials option.ClientOption
	if config.Credentials != "" {
		credentials = option.WithCredentialsFile(config.Credentials)
	}
	ctx, cancel := context.WithCancel(context.Background())
	app, err := firebase.NewApp(ctx, firebaseConfig, credentials)
	if err != nil {
		return nil, err
	}

	client, err := app.Database(ctx)
	if err != nil {
		return nil, err
	}
	_, name := toolbox.URLSplit(firebaseConfig.DatabaseURL)
	var mgoConnection = &connection{dbName: name, ctx: &ctx, cancelCtx: cancel, app: app, client: client}

	var super = dsc.NewAbstractConnection(config, p.ConnectionProvider.ConnectionPool(), mgoConnection)
	mgoConnection.AbstractConnection = super
	return mgoConnection, nil
}

func newConnectionProvider(config *dsc.Config) dsc.ConnectionProvider {
	if config.MaxPoolSize == 0 {
		config.MaxPoolSize = 1
	}
	aerospikeConnectionProvider := &connectionProvider{}
	var connectionProvider dsc.ConnectionProvider = aerospikeConnectionProvider
	var super = dsc.NewAbstractConnectionProvider(config, make(chan dsc.Connection, config.MaxPoolSize), connectionProvider)
	aerospikeConnectionProvider.AbstractConnectionProvider = super
	aerospikeConnectionProvider.AbstractConnectionProvider.ConnectionProvider = connectionProvider
	return aerospikeConnectionProvider
}
