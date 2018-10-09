package db

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"testing"
	"time"
)

type TestTable struct {
	Id   int    `json:"id" orm:""`
	Name string `json:"name"`
}

func TestDB(t *testing.T) {
	conn, err := Dial("127.0.0.1:3301", "myusername", "mysecretpassword")
	if err != nil {
		t.Error(err)
	}
	defer conn.Close()
	err = conn.Execute(`
		CREATE TABLE test (
			ID INTEGER,
			NAME VARCHAR (255),
			PRIMARY KEY (ID)
		);
	`, []interface{}{})
	if err != nil {
		logrus.Error(err)
	}

	err = conn.Execute(fmt.Sprintf(`INSERT INTO test(ID,NAME)VALUES(%d,'%s');`, 14, "nihao"), []interface{}{})

	if err != nil {
		logrus.Error(err)
	}

	rows, err := conn.Query(`select ID,NAME from test;`, []interface{}{})
	if err != nil {
		t.Error(err)
	}
	for key := range rows {
		var id int
		var name string
		rows[key].Scan(&id, &name)
		logrus.Info(id, " ", name)
	}

	re, err := conn.Query2Map("test", "", "ID", "NAME")
	logrus.Info(re, err)

}

func TestGetFieldName(t *testing.T) {
	res := GetFieldName(&TestTable{Id: 1, Name: "hello"})
	logrus.Info(res)
}

func TestGetTableName(t *testing.T) {
	logrus.Info(GetTableName(&TestTable{}))
}
