package fbc

import (
	"github.com/viant/dsc"
)

func register() {
	dsc.RegisterManagerFactory("fbc", newManagerFactory())
	dsc.RegisterDatastoreDialect("fbc", newDialect())
}

func init() {
	register()
}
