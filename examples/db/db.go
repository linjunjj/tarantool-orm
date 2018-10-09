package db

import "tarantool-orm"

var tar *db.Conn
var err error

func Init() {
	//连接到tarantool数据库
	tar, err = db.Dial("", "", "")
	if err != nil {
		panic(err)
	}

}
func GetDBInstance() *db.Conn {
	return tar
}