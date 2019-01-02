package fbc_test

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/viant/assertly"
	"github.com/viant/dsc"
	"log"
	"testing"
)

type User struct {
	Id   int    `column:"id"`
	Name string `column:"name"`
}

func TestManager(t *testing.T) {
	config, err := getTestConfig(t)
	if config == nil {
		return
	}
	if !assert.Nil(t, err) {
		return
	}
	factory := dsc.NewManagerFactory()
	manager, err := factory.Create(config)
	if err != nil {
		fmt.Printf("make sure mongodb is runnig on localhost")
		return
	}

	//Test insert
	dialect := dsc.GetDatastoreDialect("fbc")
	_ = dialect.DropTable(manager, "", "users")
	for i := 0; i < 3; i++ {
		sqlResult, err := manager.Execute("INSERT INTO users(id, name) VALUES(?, ?)", i, fmt.Sprintf("Name %d", i))
		assert.Nil(t, err)
		affected, _ := sqlResult.RowsAffected()
		assert.EqualValues(t, 1, affected)
	}

	queryCases := []struct {
		description string
		SQL         string
		parameters  []interface{}
		expect      interface{}
		hasError    bool
	}{
		{
			description: "Read records ",
			SQL:         "SELECT id, name FROM users",
			expect: []*User{
				{
					Id:   0,
					Name: "Name 0",
				},
				{
					Id:   1,
					Name: "Name 1",
				},
				{
					Id:   2,
					Name: "Name 2",
				},
			},
		},
		{
			description: "Read single with placeholder",
			SQL:         "SELECT id, name FROM users WHERE id = ?",
			parameters:  []interface{}{2},
			expect: []*User{
				{
					Id:   2,
					Name: "Name 2",
				},
			},
		},
		{
			description: "Read single with number constant",
			SQL:         "SELECT id, name FROM users WHERE id = 2",
			expect: []*User{
				{
					Id:   2,
					Name: "Name 2",
				},
			},
		},
		{
			description: "Read with text constant",
			SQL:         "SELECT id, name FROM users WHERE id = '2'",
			expect: []*User{
				{
					Id:   2,
					Name: "Name 2",
				},
			},
		},

		{
			description: "Read records  with in operator",
			SQL:         "SELECT id, name FROM users WHERE id IN(?, ?)",
			parameters:  []interface{}{1, 2},

			expect: []*User{
				{
					Id:   1,
					Name: "Name 1",
				},
				{
					Id:   2,
					Name: "Name 2",
				},
			},
		},
		{
			description: "Read records  with !=",
			SQL:         "SELECT id, name FROM users WHERE id != 0",
			parameters:  []interface{}{0, 4},
			hasError:    true,
		},
	}

	for _, useCase := range queryCases {
		var records = make([]*User, 0)
		err = manager.ReadAll(&records, useCase.SQL, useCase.parameters, nil)
		if useCase.hasError {
			assert.NotNil(t, err, useCase.description)
			continue
		}
		if !assert.Nil(t, err, useCase.description) {
			continue
		}

		assertly.AssertValues(t, useCase.expect, records, useCase.description)
	}

	{ //Test persist
		var records = []*User{
			{
				Id:   1,
				Name: "Name 1",
			},
			{
				Id:   2,
				Name: "Name 22",
			},

			{
				Id:   5,
				Name: "Name 5",
			},
		}

		inserted, updated, err := manager.PersistAll(&records, "users", nil)
		if !assert.Nil(t, err) {
			return
		}
		assert.EqualValues(t, 1, inserted)
		assert.EqualValues(t, 2, updated)
	}

	{
		sqlResult, err := manager.Execute("DELETE FROM users WHERE id IN (?, ?)", 1, 5)
		if !assert.Nil(t, err) {
			log.Print(err)
			return
		}
		affected, _ := sqlResult.RowsAffected()
		assert.EqualValues(t, 2, affected)
		var result = make([]map[string]interface{}, 0)
		err = manager.ReadAll(&result, "SELECT id, name FROM users", nil, nil)
		assert.Nil(t, err)
		assert.Equal(t, 3, len(result))
	}

}
