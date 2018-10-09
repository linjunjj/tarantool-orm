package main

import (
	db2 "tarantool-orm"
	"tarantool-orm/examples/db"
)

type Data struct {
	Name      string `json:"name"`
	IdNo      string `json:"id_no" orm:"primary_key"`

}

func (*Data) TableName() string {
	return "TBL_CARDHOLDER_BLACKLIST"
}
var (
	handlerInterfacesMap = make(map[string]interface{})

)


func main()  {

	// 初始化数据库
	db.Init()
//更新方法
	v, _ := handlerInterfacesMap[""]
	tableInter, _ := v.(db2.TableInterface)
	err := db.GetDBInstance().Update(tableInter, false)
	if err != nil {
		return
	}

//插入方法

	err1 := db.GetDBInstance().Insert(tableInter)
	if err1 != nil {
		return
	}
}