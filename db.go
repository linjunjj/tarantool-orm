package db

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"github.com/tarantool/go-tarantool"
	"reflect"
	"strconv"
	"strings"
	"time"
)

var NotFoundErr = fmt.Errorf("no match result !")

type Conn struct {
	taConn *tarantool.Connection
}

func (conn *Conn) Close() {
	conn.taConn.Close()
}

//GetTarConn 获取 tarantool数据库的连接对象
func (conn *Conn) GetTarConn() *tarantool.Connection {
	return conn.taConn
}

func Dial(addr, username, password string) (*Conn, error) {
	conn := &Conn{}
	err := conn.Dial(addr, username, password)
	return conn, err
}

func (conn *Conn) Dial(addr, username, password string) error {
	opts := tarantool.Opts{User: username, Pass: password}
	var err error
	conn.taConn, err = tarantool.Connect(addr, opts)
	return err
}

func (conn *Conn) Execute(sql string, args ...interface{}) error {
	logrus.Println(sql)
	_, err := conn.taConn.Eval(`return box.sql.execute([==[`+sql+`]==])`, []interface{}{})
	return err
}

func (conn *Conn) Query(sql string, args ...interface{}) ([]*Row, error) {
	resp, err := conn.taConn.Eval(`return box.sql.execute([==[`+sql+`]==])`, []interface{}{})
	var rows []*Row

	if err != nil {
		return nil, err
	}
	rowsO := resp.Data[0].([]interface{})
	for key := range rowsO {
		tmp := &Row{}
		tmp.content = rowsO[key].([]interface{})
		rows = append(rows, tmp)
	}
	return rows, err
}

type Row struct {
	content []interface{}
}

type Channels struct {
	Content []interface{}
}

func (row *Row) Scan(args ...interface{}) {
	for key := range args {
		//如果scan的数量超过了拥有的数量
		if key >= len(row.content) {
			break
		}

		switch s := row.content[key].(type) {
		case string:
			switch d := args[key].(type) {
			case *string:
				*d = s
			}
			break
		case uint64:
			switch d := args[key].(type) {
			case *int64:
				*d = int64(s)
				break
			case *int:
				*d = int(s)
				break
			case *uint:
				*d = uint(s)
				break
			}
			break
		case time.Time:
			switch d := args[key].(type) {
			case *time.Time:
				*d = s
				break
			}
		}

	}
}

func (row *Row) ScanAll(i interface{}) {

}

func (conn *Conn) Query2Map(tableName string, other string, elem ...string) ([]map[string]interface{}, error) {
	var result []map[string]interface{}
	//拼接参数名
	query := ""
	for _, v := range elem {
		query += "," + v
	}
	rows, err := conn.Query(`select `+query[1:]+` from `+tableName+` `+other, []interface{}{})
	logrus.Info(`select ` + query[1:] + ` FROM ` + tableName + ` ` + other)
	if err != nil {
		return nil, err
	}
	for key := range rows {
		m := map[string]interface{}{}
		i := 0
		for _, v := range elem {
			v = strings.ToLower(v)
			m[v] = rows[key].content[i]
			i++
		}
		result = append(result, m)
	}
	return result, err
}

// 获取结构体的字段名
func GetFieldName(value interface{}) string {
	v := reflect.ValueOf(value).Elem()
	t := reflect.ValueOf(value).Elem().Type()
	res := ""
	for i := 0; i < v.NumField(); i++ {
		f := t.Field(i)
		res += "," + strings.ToUpper(f.Name)
	}

	return res[1:]
}

// 获取表名
func GetTableName(value interface{}) string {
	v := reflect.ValueOf(value).Type()
	return snakeString(v.Elem().Name())
}

// snake string, XxYy to xx_yy , XxYY to xx_yy
func snakeString(s string) string {
	data := make([]byte, 0, len(s)*2)
	j := false
	num := len(s)
	for i := 0; i < num; i++ {
		d := s[i]
		if i > 0 && d >= 'A' && d <= 'Z' && j {
			data = append(data, '_')
		}
		if d != '_' {
			j = true
		}
		data = append(data, d)
	}
	return strings.ToUpper(string(data[:]))
}

func Result2Map() {

}

func (conn *Conn) QueryUsableChannels(sql string) ([]*Channels, error) {
	resp, err := conn.taConn.Eval(`return box.sql.execute([==[`+sql+`]==])`, []interface{}{})
	var rows []*Channels

	if err != nil {
		return nil, err
	}
	rowsO := resp.Data[0].([]interface{})
	for key := range rowsO {
		tmp := &Channels{}
		tmp.Content = rowsO[key].([]interface{})
		rows = append(rows, tmp)
	}

	return rows, err
}

type StructField struct {
	Struct    reflect.StructField
	Value     reflect.Value
	Name      string
	OrmTag    string
	JsonTag   string
	SearchTag string
	IsBlank   bool
	Num       string
}

func (conn *Conn) SelectOne(tableInter TableInterface) (interface{}, error) {
	tableName := tableInter.TableName()
	v := tableInter.(interface{})
	structMap, err := ParseStruct(v)
	if err != nil {
		return nil, err
	}
	result, err := conn.search(tableName, Option{Limit: 1}, structMap)
	if err != nil {
		return nil, err
	}
	if len(result) < 1 {
		return nil, NotFoundErr
	}
	for key, value := range result[0] {
		if value != nil {
			if _, ok := structMap[key]; ok {
				structMap[key].Value.Set(reflect.ValueOf(value))
			} else {
				logrus.Errorf("------db set value: the %s is not exist !", key)
			}
		}
	}
	return v, nil
}

type Option struct {
	Limit int
	Skip  int
	Order string //key1 AEC, key2 DEC
}

type TableInterface interface {
	TableName() string
}

func (conn *Conn) SelectMany(tableInter TableInterface, op Option) ([]interface{}, error) {
	tableName := tableInter.TableName()
	v := tableInter.(interface{})
	structMap, err := ParseStruct(v)
	if err != nil {
		return nil, err
	}
	result, err := conn.search(tableName, op, structMap)
	if err != nil {
		return nil, err
	}
	var structArray []interface{}
	var structFdsMap []map[string]StructField
	for i := 0; i < len(result); i++ {
		rv := reflect.New(reflect.TypeOf(v).Elem())
		rv.Elem().Set(reflect.ValueOf(v).Elem())
		val := rv.Interface()
		structArray = append(structArray, val)
		m, _ := ParseStruct(val)
		structFdsMap = append(structFdsMap, m)
	}
	if len(result) < 1 {
		return nil, NotFoundErr
	}
	for i := 0; i < len(result); i++ {
		for key, value := range result[i] {
			if value != nil {
				inter := valueTransferFromDB(value, structFdsMap[i][key])
				if _, ok := structFdsMap[i][key]; ok {
					structFdsMap[i][key].Value.Set(reflect.ValueOf(inter))
				} else {
					logrus.Errorf("------db set value: the %s is not exist !", key)
				}
			}
		}
	}
	return structArray, nil
}

func (conn *Conn) Search(tableInter TableInterface, op Option) ([]map[string]interface{}, error) {
	tableName := tableInter.TableName()
	v := tableInter.(interface{})
	structMap, err := ParseStruct(v)
	if err != nil {
		return nil, err
	}
	result, err := conn.searchLike(tableName, op, structMap)
	if err != nil {
		return nil, err
	}
	if len(result) < 1 {
		return nil, NotFoundErr
	}
	var retArray []map[string]interface{}
	for i, _ := range result {
		retMap := make(map[string]interface{})
		for key, value := range result[i] {
			retMap[strings.ToLower(key)] = value
		}
		retArray = append(retArray, retMap)
	}
	return retArray, nil
}

func (conn *Conn) searchWithSQL(tableName, other string, op Option, stuctFdMap map[string]StructField) ([]map[string]interface{}, error) {
	if op.Limit == 0 {
		op.Limit = 999
	}
	//拼接参数名
	query := ""
	var selectFds []string
	for tag, _ := range stuctFdMap {
		selectFds = append(selectFds, tag)
		query += "," + tag
	}
	other += fmt.Sprintf(" limit %d,%d", op.Skip, op.Limit)

	var result []map[string]interface{}
	rows, err := conn.Query(`select `+query[1:]+` from `+tableName+` `+other, []interface{}{})
	logrus.Info(`select ` + query[1:] + ` FROM ` + tableName + ` ` + other)
	if err != nil {
		return nil, err
	}
	for key := range rows {
		m := map[string]interface{}{}
		for i, v := range selectFds {
			m[v] = rows[key].content[i]
		}
		result = append(result, m)
	}
	return result, nil
}

func (conn *Conn) search(tableName string, op Option, stuctFdMap map[string]StructField) ([]map[string]interface{}, error) {
	if op.Limit == 0 {
		op.Limit = 999
	}
	var selectFds []string
	condition := ""
	for tag, field := range stuctFdMap {
		if !field.IsBlank {
			val := valueTransferToDB(field)
			condition += fmt.Sprintf(" %s = %s and", strings.ToUpper(tag), val)
		} else {
			selectFds = append(selectFds, strings.ToUpper(tag))
		}
	}
	if condition != "" {
		condition = condition[:strings.LastIndex(condition, "and")]
		condition = "where" + condition
	}
	condition += fmt.Sprintf(" limit %d,%d", op.Skip, op.Limit)
	return conn.Query2Map(tableName, condition, selectFds...)
}

func (conn *Conn) searchLike(tableName string, op Option, stuctFdMap map[string]StructField) ([]map[string]interface{}, error) {
	if op.Limit == 0 {
		op.Limit = 999
	}
	var selectFds []string
	condition := ""
	for tag, field := range stuctFdMap {
		if !field.IsBlank {
			if field.Struct.Type.Kind() != reflect.String {
				return nil, fmt.Errorf("only support type(string) search")
			}
			val := fmt.Sprintf("%s", field.Value)
			//condition += fmt.Sprintf(" %s like '%%s%' and", strings.ToUpper(tag), val)
			condition += strings.ToUpper(tag) + " like '%" + val + "%' and "
		}
		selectFds = append(selectFds, strings.ToUpper(tag))
	}
	if condition != "" {
		condition = condition[:strings.LastIndex(condition, "and")]
		condition = "where " + condition
	} else {
		return nil, fmt.Errorf("the condition is empty !")
	}
	condition += fmt.Sprintf(" limit %d,%d", op.Skip, op.Limit)
	return conn.Query2Map(tableName, condition, selectFds...)
}

func ParseStruct(v interface{}) (map[string]StructField, error) {
	structMap := make(map[string]StructField)
	reflectType := reflect.TypeOf(v).Elem()
	reflectValue := reflect.ValueOf(v)
	reflectKind := reflectType.Kind()
	if reflectKind != reflect.Struct {
		return nil, fmt.Errorf("not support non-struct type: %s !", reflectKind.String())
	}
	for i := 0; i < reflectType.NumField(); i++ {
		fieldStruct := reflectType.Field(i)
		fieldValue := reflectValue.Elem().Field(i)
		if fieldStruct.Type.Kind() == reflect.Struct {
			rfValue := reflect.New(reflect.PtrTo(fieldStruct.Type))
			rfValue.Elem().Set(fieldValue.Addr())
			tagMap, err := ParseStruct(rfValue.Elem().Interface())
			if err != nil {
				return nil, err
			}
			for key, value := range tagMap {
				structMap[key] = value
			}
			continue
		}
		ormTag := fieldStruct.Tag.Get("orm")
		jsonTag := fieldStruct.Tag.Get("json")
		searchTag := fieldStruct.Tag.Get("search")
		if strings.Contains(ormTag, "-") {
			continue
		}
		field := StructField{
			Struct:    fieldStruct,
			Name:      fieldStruct.Name,
			OrmTag:    ormTag,
			JsonTag:   jsonTag,
			SearchTag: searchTag,
			Value:     fieldValue,
			IsBlank:   isBlank(fieldValue),
			Num:       strconv.Itoa(i),
		}
		//		fmt.Println("field:", field)
		if searchTag != "" {
			structMap[searchTag] = field
		} else if ormTag != "" && ormTag != "primary_key" {
			structMap[ormTag] = field
		} else {
			structMap[jsonTag] = field
		}
	}
	return structMap, nil
}

func (conn *Conn) Insert(tableInter TableInterface) error {
	tableName := tableInter.TableName()
	v := tableInter.(interface{})
	stuctFdMap, err := ParseStruct(v)
	if err != nil {
		return err
	}
	var para1, para2 string
	for tag, structFd := range stuctFdMap {
		if !structFd.IsBlank {
			para1 += strings.ToUpper(tag) + ","
			val := valueTransferToDB(structFd)
			para2 += val + ","
		}
	}
	para1 = strings.TrimSuffix(para1, ",")
	para2 = strings.TrimSuffix(para2, ",")

	return conn.Execute(fmt.Sprintf("insert into %s (%s) VALUES (%s)", tableName, para1, para2))
}

func (conn *Conn) Update(tableInter TableInterface, force bool) error {
	if force {
		logrus.Warn("启用强制更新")
	}
	tableName := tableInter.TableName()
	v := tableInter.(interface{})
	stuctFdMap, err := ParseStruct(v)
	if err != nil {
		return err
	}
	condition := ""
	updates := ""
	for tag, field := range stuctFdMap {
		if field.IsBlank && field.OrmTag == "primary_key" {
			logrus.Errorf("the primary_key field %s is empty !\n", field.Name)
			if !force {
				return fmt.Errorf("the primary_key field %s is empty !\n", field.Name)
			}
		}
		if !field.IsBlank {
			val := valueTransferToDB(field)
			if field.OrmTag == "primary_key" {
				condition += fmt.Sprintf(" %s = %s and", strings.ToUpper(tag), val)
			} else {
				updates += fmt.Sprintf(" %s = %s ,", strings.ToUpper(tag), val)
			}
		}
	}
	if condition != "" {
		condition = condition[:strings.LastIndex(condition, "and")]
		condition = "where" + condition
	} else {
		return fmt.Errorf("sql: the where condition is empty !\n")
	}
	if updates != "" {
		updates = updates[:strings.LastIndex(updates, ",")]
		updates = "set" + updates
	}
	return conn.Execute(fmt.Sprintf("update %s %s %s", tableName, updates, condition))
}

//用于个表数据更新
func (conn *Conn) UpdateManyTables(tableName, sqlOther string, key string, force bool) error {
	if force {
		logrus.Warn("启用强制更新")
	}
	data := sqlOther[:strings.LastIndex(sqlOther, "where")]
	if !force && strings.Contains(data, key) {
		return fmt.Errorf("the primary_key field not change ")
	}

	return conn.Execute(fmt.Sprintf("update %s %s", tableName, sqlOther))
}

func valueTransferToDB(field StructField) string {
	val := ""
	switch field.Struct.Type.Kind() {
	case reflect.Int, reflect.Int64, reflect.Int32:
		val = fmt.Sprintf("%d", field.Value.Int())
	case reflect.Uint64, reflect.Uint32:
		val = fmt.Sprintf("%d", field.Value.Uint())
	case reflect.Float64, reflect.Float32:
		val = fmt.Sprintf("%.2f", field.Value.Float())
	default:
		val = fmt.Sprintf("'%s'", field.Value)
	}
	return val
}

func valueTransferFromDB(dbValue interface{}, field StructField) interface{} {
	oriVal := dbValue
	value := dbValue
	// 如果查询出来的类型和 struct的类型对不上
	if reflect.TypeOf(dbValue).Kind() != field.Struct.Type.Kind() {
		// 判断 value的类型
		switch reflect.TypeOf(dbValue).Kind() {
		case reflect.Uint64, reflect.Uint32, reflect.Int64, reflect.Int, reflect.Int32:
			// 判断 struct 字段对应的类型
			switch field.Struct.Type.Kind() {
			case reflect.Float64, reflect.Float32:
				value, _ = strconv.ParseFloat(fmt.Sprint(value), 64)
				return value
			case reflect.Int, reflect.Int32:
				value, _ = strconv.Atoi(fmt.Sprint(value))
			case reflect.Uint64, reflect.Int64:
				value, _ = strconv.ParseInt(fmt.Sprint(value), 10, 64)
			default:
				value = fmt.Sprint(value)
			}
		default:
			value = fmt.Sprintf("'%s'", value)
		}
		if fmt.Sprint(value) != fmt.Sprint(oriVal) {
			fmt.Printf("#tag---%s\n#dbType---%s\n#dstType---%s\n#value---%s\n#origin_val---%s\n", field.JsonTag, reflect.TypeOf(dbValue).Kind(), field.Struct.Type.Kind(), value, oriVal)
		}
	}
	return value
}

func (conn *Conn) SelectCount(tableInter TableInterface) (int, error) {
	tableName := tableInter.TableName()
	v := tableInter.(interface{})
	structMap, err := ParseStruct(v)
	if err != nil {
		return 0, err
	}
	var selectFds []string
	condition := ""
	for tag, field := range structMap {
		if !field.IsBlank {
			val := valueTransferToDB(field)
			condition += fmt.Sprintf(" %s = %s and", strings.ToUpper(tag), val)
		} else {
			selectFds = append(selectFds, strings.ToUpper(tag))
		}
	}
	if condition != "" {
		condition = condition[:strings.LastIndex(condition, "and")]
		condition = "where" + condition
	}
	rows, err := conn.Query(fmt.Sprintf("select count(*) from %s %s", tableName, condition))
	if err != nil {
		return 0, err
	}
	//fmt.Println(rows[0].content[0])
	count, _ := strconv.Atoi(fmt.Sprint(rows[0].content[0]))
	return count, nil
}

func (conn *Conn) SelectCountWithTables(tableName, sqlOther string) (int, error) {
	sql := fmt.Sprintf("select count(*) from %s ", tableName)
	sql += sqlOther
	logrus.Info(sql)
	rows, err := conn.Query(sql)
	if err != nil {
		return 0, err
	}
	count, _ := strconv.Atoi(fmt.Sprint(rows[0].content[0]))
	return count, nil
}

func (conn *Conn) Delete(tableInter TableInterface, force bool) error {
	if force {
		logrus.Warn("启用强制删除")
	}
	tableName := tableInter.TableName()
	v := tableInter.(interface{})
	structMap, err := ParseStruct(v)
	if err != nil {
		return err
	}
	condition := ""
	for tag, structFd := range structMap {
		if !structFd.IsBlank {
			val := valueTransferToDB(structFd)
			condition += fmt.Sprintf(" %s = %s and", strings.ToUpper(tag), val)
		} else if structFd.OrmTag == "primary_key" && !force { //主键为空不允许删除
			return fmt.Errorf("the primary_key %s is empty", tag)
		}
	}
	if condition != "" {
		condition = condition[:strings.LastIndex(condition, "and")]
		condition = "where" + condition
	} else {
		return fmt.Errorf("no where condition !")
	}
	return conn.Execute(fmt.Sprintf("delete from %s %s", tableName, condition))
}
func isBlank(value reflect.Value) bool {
	switch value.Kind() {
	case reflect.String:
		return value.Len() == 0
	case reflect.Bool:
		return !value.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return value.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return value.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return value.Float() == 0
	case reflect.Interface, reflect.Ptr:
		return value.IsNil()
	}
	return reflect.DeepEqual(value.Interface(), reflect.Zero(value.Type()).Interface())
}

func (conn *Conn) Query3Map(sql string, elem ...string) ([]map[string]interface{}, error) {
	var result []map[string]interface{}

	rows, err := conn.Query(sql, []interface{}{})
	logrus.Info(sql)
	if err != nil {
		return nil, err
	}

	for key := range rows {
		fmt.Println("key:", key)
		m := map[string]interface{}{}
		i := 0
		for _, v := range elem {
			v = strings.ToLower(v)
			m[v] = rows[key].content[i]
			i++
		}
		result = append(result, m)
	}
	return result, err
}

func (conn *Conn) DeleteSPMerchant(tableName string, other string) error {

	_, err := conn.Query(`delete from ` + tableName + ` ` + other)
	logrus.Info(`delete FROM ` + tableName + ` ` + other)
	if err != nil {
		return err
	}

	return nil
}

/*
根据传入完整的sql命令查询，适用于复杂多表查询
*/
func (conn *Conn) SelectManyFromTables(tableName, sqlOther string, op Option, v interface{}) ([]interface{}, error) {
	structMap, err := ParseStruct(v)
	if err != nil {
		return nil, err
	}
	result, err := conn.searchWithSQL(tableName, sqlOther, op, structMap)
	if err != nil {
		return nil, err
	}
	var structArray []interface{}
	var structFdsMap []map[string]StructField
	for i := 0; i < len(result); i++ {
		rv := reflect.New(reflect.TypeOf(v).Elem())
		rv.Elem().Set(reflect.ValueOf(v).Elem())
		val := rv.Interface()
		structArray = append(structArray, val)
		m, _ := ParseStruct(val)
		structFdsMap = append(structFdsMap, m)
	}
	if len(result) < 1 {
		return nil, NotFoundErr
	}

	for i := 0; i < len(result); i++ {
		for key, value := range result[i] {
			if value != nil {
				inter := valueTransferFromDB(value, structFdsMap[i][key])
				if _, ok := structFdsMap[i][key]; ok {
					structFdsMap[i][key].Value.Set(reflect.ValueOf(inter))
				} else {
					logrus.Errorf("------db set value: the %s is not exist !", key)
				}
			}
		}
	}
	return structArray, nil
}

/*
根据传入完整的sql命令，适用多表搜索
*/

//func (conn *Conn)SearchManyFromTables(tableName,sqlOther string,op Option,v interface{})([]interface{},error){
//	if op.Limit==0{
//		op.Limit=20
//	}
//	structMap,err:=ParseStruct(v)
//	if err!=nil {
//		return nil,err
//	}
//	query:=""
//	var selectFds []string
//	for tag, _ := range structMap {
//		selectFds = append(selectFds, tag)
//		query += "," + tag
//	}
//	sqlOther += fmt.Sprintf(" limit %d,%d", op.Skip, op.Limit)
//
//
//
//}

func (conn *Conn) searchFromTables(sql string, op Option, stuctFdMap map[string]StructField) ([]map[string]interface{}, error) {
	if op.Limit == 0 {
		op.Limit = 999
	}
	var selectFds []string

	// 字段根据排序
	for i := 0; i < len(stuctFdMap); i++ {
		for tag, field := range stuctFdMap {
			if field.Num == strconv.Itoa(i) {
				selectFds = append(selectFds, strings.ToUpper(tag))
				continue
			}
		}
	}

	sql += fmt.Sprintf(" limit %d,%d", op.Skip, op.Limit)
	return conn.Query3Map(sql, selectFds...)
}

type Join struct {
	JoinSql  string
	Align    string
	FieldMap map[string]StructField
}

func (conn *Conn) JoinFunc(joinSql, condition string, op Option, stuctFdMap map[string]StructField, v interface{}) ([]interface{}, error) {
	result, err := conn.searchWithSQL(joinSql, condition, op, stuctFdMap)
	if err != nil {
		return nil, err
	}
	var structArray []interface{}
	var structFdsMap []map[string]StructField
	for i := 0; i < len(result); i++ {
		rv := reflect.New(reflect.TypeOf(v).Elem())
		val := rv.Interface()
		structArray = append(structArray, val)
		m, _ := ParseStruct(val)
		structFdsMap = append(structFdsMap, m)
	}
	if len(result) < 1 {
		return nil, NotFoundErr
	}

	for i := 0; i < len(result); i++ {
		for key, value := range result[i] {
			if value != nil {
				ks := strings.Split(key, ".")
				if len(ks) == 2 {
					key = ks[1]
				}
				if _, ok := structFdsMap[i][key]; ok {
					inter := valueTransferFromDB(value, structFdsMap[i][key])
					structFdsMap[i][key].Value.Set(reflect.ValueOf(inter))
				} else {
					logrus.Errorf("------db set value: the %s is not exist !", key)
				}
			}
		}
	}
	return structArray, nil
}

func (conn *Conn) JoinSearch(v interface{}, op Option, joins ...Join) ([]interface{}, error) {
	tableFds := ""
	condition := ""
	structFdMap := make(map[string]StructField)
	for i, value := range joins {
		tableFds += " " + value.JoinSql
		for tag, val := range value.FieldMap {
			newTag := fmt.Sprintf("%s.%s", value.Align, tag)
			if _, ok := structFdMap[newTag]; ok {
				panic(fmt.Sprintf("the tag %s is exist !", newTag))
			}
			structFdMap[newTag] = joins[i].FieldMap[tag]
			if !val.IsBlank {
				if val.Struct.Type.Kind() != reflect.String {
					return nil, fmt.Errorf("only support type(string) search")
				}
				condition += fmt.Sprintf("%s.%s", value.Align, tag) + " like '%" + val.Value.String() + "%' and "
			}
		}
	}
	if condition != "" {
		condition = condition[:strings.LastIndex(condition, "and")]
		condition = " where " + condition
	} else {
		return nil, fmt.Errorf("where condition is empty !")
	}
	return conn.JoinFunc(tableFds, condition, op, structFdMap, v)
}
