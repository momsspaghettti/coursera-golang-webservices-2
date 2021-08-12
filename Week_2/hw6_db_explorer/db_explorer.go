package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
)

// тут вы пишете код
// обращаю ваше внимание - в этом задании запрещены глобальные переменные

type handlerHelper struct {
	Method     string
	PathRegexp *regexp.Regexp
	Handler    func(w http.ResponseWriter, r *http.Request)
}

type dbColumn struct {
	Name      string
	Type      string
	AllowNull bool
	IsKey     bool
}

type dbColumns []*dbColumn

type dbTable struct {
	Columns *dbColumns
	KeyName string
}

type dbTables struct {
	TablesNamesList []string
	TablesMap       map[string]*dbTable
}

type DbExplorer struct {
	Db             *sql.DB
	DbTables       *dbTables
	HandlerHelpers *[]*handlerHelper
}

func NewDbExplorer(db *sql.DB) (*DbExplorer, error) {
	db.SetMaxIdleConns(1)

	dbExplorer := &DbExplorer{Db: db}
	dbExplorer.buildHandlerHelpers()

	if err := dbExplorer.buildDbSchema(db); err != nil {
		return nil, err
	}

	return dbExplorer, nil
}

func (dbExplorer *DbExplorer) buildHandlerHelpers() {
	handlerHelpers := make([]*handlerHelper, 6)

	// GET /
	handlerHelpers[0] = &handlerHelper{
		"GET",
		regexp.MustCompile(`^/$`),
		dbExplorer.getAllTablesHandler,
	}

	// GET /$table
	handlerHelpers[1] = &handlerHelper{
		"GET",
		regexp.MustCompile(`^/[^/ ]+$`),
		dbExplorer.getTableRecordsHandler,
	}

	// GET /$table/$id
	handlerHelpers[2] = &handlerHelper{
		"GET",
		regexp.MustCompile(`^/[^/ ]+/\d+$`),
		dbExplorer.getTableRecordHandler,
	}

	// PUT /$table
	handlerHelpers[3] = &handlerHelper{
		"PUT",
		regexp.MustCompile(`^/[^/ ]+/$`),
		dbExplorer.createTableRecordHandler,
	}

	// POST /$table/$id
	handlerHelpers[4] = &handlerHelper{
		"POST",
		regexp.MustCompile(`^/[^/ ]+/\d+$`),
		dbExplorer.updateTableRecordHandler,
	}

	// DELETE /$table/$id
	handlerHelpers[5] = &handlerHelper{
		"DELETE",
		regexp.MustCompile(`^/[^/ ]+/\d+$`),
		dbExplorer.deleteTableRecordHandler,
	}

	dbExplorer.HandlerHelpers = &handlerHelpers
}

func (dbExplorer *DbExplorer) buildDbSchema(db *sql.DB) error {
	tables := dbTables{
		make([]string, 0, 64),
		make(map[string]*dbTable, 64),
	}

	tablesRows, err := db.Query("SHOW TABLES")
	if err != nil {
		return err
	}
	defer func() {
		_ = tablesRows.Close()
	}()

	var tableName string
	for tablesRows.Next() {
		if err = tablesRows.Scan(&tableName); err != nil {
			break
		}
		tables.TablesNamesList = append(tables.TablesNamesList, tableName)
		table, err := buildDbTable(db, tableName)
		if err != nil {
			return err
		}
		tables.TablesMap[tableName] = table
	}

	dbExplorer.DbTables = &tables
	return nil
}

func buildDbTable(db *sql.DB, tableName string) (*dbTable, error) {
	columns := dbColumns(make([]*dbColumn, 0, 64))

	columnsRows, err := db.Query("SHOW COLUMNS FROM " + tableName)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = columnsRows.Close()
	}()

	var keyName string
	var f, t, n, k, e string
	var d *string
	for columnsRows.Next() {
		if err = columnsRows.Scan(&f, &t, &n, &k, &d, &e); err != nil {
			return nil, err
		}
		column := dbColumn{
			Name: f,
			Type: getColumnType(t),
		}
		if n == "YES" {
			column.AllowNull = true
		}

		if k != "" {
			column.IsKey = true
			keyName = f
		}
		columns = append(columns, &column)
	}

	if keyName == "" {
		return nil, fmt.Errorf("can not find key in table")
	}

	return &dbTable{&columns, keyName}, nil
}

func getColumnType(rawType string) string {
	if strings.Contains(rawType, "char") ||
		strings.Contains(rawType, "varchar") ||
		strings.Contains(rawType, "text") ||
		strings.Contains(rawType, "blob") {
		return "string"
	}

	if strings.Contains(rawType, "int") {
		return "int"
	}

	return "float"
}

func (dbExplorer *DbExplorer) checkAndGetHandler(path, method string) (func(w http.ResponseWriter, r *http.Request), bool) {
	for _, helper := range *dbExplorer.HandlerHelpers {
		if helper.Method == method && helper.PathRegexp.MatchString(path) {
			return helper.Handler, true
		}
	}
	return nil, false
}

func (dbExplorer *DbExplorer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	handler, found := dbExplorer.checkAndGetHandler(r.URL.Path, r.Method)
	if found {
		handler(w, r)
	} else {
		writeResponse(w, http.StatusNotFound, serverResponse{Err: "method not found"})
	}
}

type response map[string]interface{}

type serverResponse struct {
	Err      string
	Response response
}

func marshalResponse(res serverResponse) ([]byte, error) {
	m := make(map[string]interface{})
	if res.Err != "" {
		m["error"] = res.Err
	} else {
		m["response"] = res.Response
	}
	return json.Marshal(m)
}

func writeResponse(w http.ResponseWriter, statusCode int, response serverResponse) {
	rawRes, err := marshalResponse(response)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
	} else {
		w.WriteHeader(statusCode)
		_, _ = w.Write(rawRes)
	}
}

func (dbExplorer *DbExplorer) getAllTablesHandler(w http.ResponseWriter, _ *http.Request) {
	res := serverResponse{
		Response: response{
			"tables": dbExplorer.getAllTables(),
		},
	}
	writeResponse(w, http.StatusOK, res)
}

func (dbExplorer *DbExplorer) getAllTables() []string {
	return dbExplorer.DbTables.TablesNamesList
}

func (dbExplorer *DbExplorer) getTableRecordsHandler(w http.ResponseWriter, r *http.Request) {
	tableName := strings.TrimPrefix(r.URL.Path, "/")
	limit := 5
	offset := 0

	if limitStr := r.FormValue("limit"); limitStr != "" {
		limitInt, err := strconv.Atoi(limitStr)
		if err == nil {
			limit = limitInt
		}
	}

	if offsetStr := r.FormValue("offset"); offsetStr != "" {
		offsetInt, err := strconv.Atoi(offsetStr)
		if err == nil {
			offset = offsetInt
		}
	}

	records, tableExists, err := dbExplorer.getTableRecords(tableName, limit, offset)
	if err != nil {
		writeResponse(w, http.StatusInternalServerError, serverResponse{Err: "failed to get records: " + err.Error()})
		return
	}

	if !tableExists {
		writeResponse(w, http.StatusNotFound, serverResponse{Err: "unknown table"})
		return
	}

	res := serverResponse{
		Response: response{
			"records": records,
		},
	}
	writeResponse(w, http.StatusOK, res)
}

func (dbExplorer *DbExplorer) getTableRecords(tableName string, limit, offset int) ([]interface{}, bool, error) {
	table, tableExists := dbExplorer.DbTables.TablesMap[tableName]
	if !tableExists {
		return nil, false, nil
	}
	columns := table.Columns

	sqlStr := "SELECT * FROM " + tableName + " LIMIT " + strconv.Itoa(limit) + " OFFSET " + strconv.Itoa(offset)
	columnsCount := len(*columns)
	records, err := dbQuery(dbExplorer.Db, sqlStr, columnsCount, limit)

	if err != nil {
		return nil, false, err
	}

	res := make([]interface{}, 0, len(records))
	for i := 0; i < len(records); i++ {
		m := make(map[string]interface{}, columnsCount)

		for j := 0; j < columnsCount; j++ {
			value := records[i][j]
			column := (*columns)[j]
			resValue, err := convertDbValue(value, column)
			if err != nil {
				return nil, false, err
			}
			m[column.Name] = resValue
		}

		res = append(res, m)
	}

	return res, true, nil
}

func (dbExplorer *DbExplorer) getTableRecordHandler(w http.ResponseWriter, r *http.Request) {
	tableName, recordId, err := getTableNameAndRecordId(r.URL.Path)

	if err != nil {
		writeResponse(w, http.StatusInternalServerError, serverResponse{Err: err.Error()})
	}

	_, tableExists := dbExplorer.DbTables.TablesMap[tableName]
	if !tableExists {
		writeResponse(w, http.StatusNotFound, serverResponse{Err: "unknown table"})
		return
	}

	record, recordExists, err := dbExplorer.getTableRecord(tableName, recordId)
	if err != nil {
		writeResponse(w, http.StatusInternalServerError, serverResponse{Err: "failed to get record: " + err.Error()})
		return
	}

	if !recordExists {
		writeResponse(w, http.StatusNotFound, serverResponse{Err: "record not found"})
		return
	}

	res := serverResponse{
		Response: response{
			"record": record,
		},
	}
	writeResponse(w, http.StatusOK, res)
}

func getTableNameAndRecordId(urlPath string) (string, int64, error) {
	pathArr := strings.Split(urlPath, "/")
	if len(pathArr) != 3 {
		return "", 0, fmt.Errorf("invalid path")
	}
	tableName := pathArr[1]
	recordId, err := strconv.ParseInt(pathArr[2], 10, 64)
	if err != nil {
		return "", 0, fmt.Errorf("failed to parse id: " + err.Error())
	}
	return tableName, recordId, nil
}

func (dbExplorer *DbExplorer) getTableRecord(tableName string, id int64) (interface{}, bool, error) {
	table, _ := dbExplorer.DbTables.TablesMap[tableName]

	sqlStr := "SELECT * FROM " + tableName + " WHERE " + table.KeyName + " = " + strconv.FormatInt(id, 10)

	columns := table.Columns
	columnsCount := len(*columns)
	records, err := dbQuery(dbExplorer.Db, sqlStr, columnsCount, 1)

	if err != nil {
		return nil, false, err
	}

	if len(records) != 1 {
		return nil, false, nil
	}

	res := make(map[string]interface{}, columnsCount)
	for i := 0; i < columnsCount; i++ {
		value := records[0][i]
		column := (*columns)[i]
		resValue, err := convertDbValue(value, column)
		if err != nil {
			return nil, false, err
		}
		res[column.Name] = resValue
	}

	return res, true, nil
}

func (dbExplorer *DbExplorer) createTableRecordHandler(w http.ResponseWriter, r *http.Request) {
	pathArr := strings.Split(r.URL.Path, "/")
	if len(pathArr) != 3 {
		writeResponse(w, http.StatusInternalServerError, serverResponse{Err: "invalid path"})
		return
	}
	tableName := pathArr[1]
	table, tableExists := dbExplorer.DbTables.TablesMap[tableName]
	if !tableExists {
		writeResponse(w, http.StatusNotFound, serverResponse{Err: "unknown table"})
		return
	}

	req, err := parseRequest(r)
	if err != nil {
		writeResponse(w, http.StatusInternalServerError, serverResponse{Err: "failed to parse request"})
		return
	}

	recordId, err := dbExplorer.createTableRecord(tableName, req)
	if err != nil {
		writeResponse(w, http.StatusBadRequest, serverResponse{Err: err.Error()})
		return
	}

	res := serverResponse{
		Response: response{
			table.KeyName: recordId,
		},
	}
	writeResponse(w, http.StatusOK, res)
}

func (dbExplorer *DbExplorer) createTableRecord(tableName string, req *request) (int64, error) {
	queryParams, err := dbExplorer.buildExecQueryParams(tableName, req, true)

	if err != nil {
		return 0, err
	}

	columnNames := queryParams.ColumnNames
	columnValues := queryParams.ColumnValues

	sqlStr := strings.Builder{}
	sqlStr.WriteString("INSERT INTO ")
	sqlStr.WriteString(tableName)
	sqlStr.WriteString(" (")
	sqlStr.WriteString(strings.Join(columnNames, ", "))
	sqlStr.WriteString(") VALUES (?")
	sqlStr.WriteString(strings.Repeat(", ?", len(columnNames)-1))
	sqlStr.WriteString(")")

	result, err := dbExplorer.Db.Exec(sqlStr.String(), columnValues...)
	if err != nil {
		return 0, err
	}

	return result.LastInsertId()
}

func (dbExplorer *DbExplorer) updateTableRecordHandler(w http.ResponseWriter, r *http.Request) {
	tableName, recordId, err := getTableNameAndRecordId(r.URL.Path)

	if err != nil {
		writeResponse(w, http.StatusInternalServerError, serverResponse{Err: err.Error()})
	}

	_, tableExists := dbExplorer.DbTables.TablesMap[tableName]
	if !tableExists {
		writeResponse(w, http.StatusNotFound, serverResponse{Err: "unknown table"})
		return
	}

	req, err := parseRequest(r)
	if err != nil {
		writeResponse(w, http.StatusInternalServerError, serverResponse{Err: "failed to parse request"})
		return
	}

	updatedCount, err := dbExplorer.updateTableRecord(tableName, recordId, req)
	if err != nil {
		writeResponse(w, http.StatusBadRequest, serverResponse{Err: err.Error()})
		return
	}

	res := serverResponse{
		Response: response{
			"updated": updatedCount,
		},
	}
	writeResponse(w, http.StatusOK, res)
}

func (dbExplorer *DbExplorer) updateTableRecord(tableName string, recordId int64, req *request) (int64, error) {
	queryParams, err := dbExplorer.buildExecQueryParams(tableName, req, false)

	if err != nil {
		return 0, err
	}

	columnNames := queryParams.ColumnNames
	columnValues := queryParams.ColumnValues

	keyName := dbExplorer.DbTables.TablesMap[tableName].KeyName

	sqlStr := strings.Builder{}
	sqlStr.WriteString("UPDATE ")
	sqlStr.WriteString(tableName)
	sqlStr.WriteString(" SET ")
	for i, columnName := range columnNames {
		sqlStr.WriteString(columnName)
		sqlStr.WriteString(" = ?")
		if i+1 < len(columnNames) {
			sqlStr.WriteString(", ")
		}
	}
	sqlStr.WriteString(" WHERE ")
	sqlStr.WriteString(keyName)
	sqlStr.WriteString(" = ")
	sqlStr.WriteString(strconv.FormatInt(recordId, 10))

	result, err := dbExplorer.Db.Exec(sqlStr.String(), columnValues...)
	if err != nil {
		return 0, err
	}

	return result.RowsAffected()
}

type execQueryParams struct {
	ColumnNames  []string
	ColumnValues []interface{}
}

func (dbExplorer *DbExplorer) buildExecQueryParams(tableName string, req *request, insertQuery bool) (*execQueryParams, error) {
	table, _ := dbExplorer.DbTables.TablesMap[tableName]

	columns := table.Columns
	columnsCount := len(*columns)

	columnNames := make([]string, 0, columnsCount)
	columnValues := make([]interface{}, 0, columnsCount)

	for i := 0; i < columnsCount; i++ {
		column := (*columns)[i]
		if insertQuery && column.IsKey {
			continue
		}

		columnName := column.Name
		var columnValue interface{}
		reqValue, exists := (*req)[columnName]

		if !insertQuery && !exists {
			continue
		}

		if !exists {
			if !column.AllowNull {
				switch column.Type {
				case "int":
					columnValue = 0
				case "float":
					columnValue = float64(0)
				default:
					columnValue = ""
				}
			}
		} else {
			if reqValue.Value == nil && !column.AllowNull ||
				reqValue.Value != nil && reqValue.Type != column.Type ||
				column.IsKey && !insertQuery {
				return nil, fmt.Errorf("field " + columnName + " have invalid type")
			}
			columnValue = reqValue.Value
		}

		columnNames = append(columnNames, columnName)
		columnValues = append(columnValues, columnValue)
	}

	return &execQueryParams{columnNames, columnValues}, nil
}

func (dbExplorer *DbExplorer) deleteTableRecordHandler(w http.ResponseWriter, r *http.Request) {
	tableName, recordId, err := getTableNameAndRecordId(r.URL.Path)

	if err != nil {
		writeResponse(w, http.StatusInternalServerError, serverResponse{Err: err.Error()})
	}

	_, tableExists := dbExplorer.DbTables.TablesMap[tableName]
	if !tableExists {
		writeResponse(w, http.StatusNotFound, serverResponse{Err: "unknown table"})
		return
	}

	deletedCount, err := dbExplorer.deleteTableRecord(tableName, recordId)
	if err != nil {
		writeResponse(w, http.StatusInternalServerError, serverResponse{Err: err.Error()})
		return
	}

	res := serverResponse{
		Response: response{
			"deleted": deletedCount,
		},
	}
	writeResponse(w, http.StatusOK, res)
}

func (dbExplorer *DbExplorer) deleteTableRecord(tableName string, recordId int64) (int64, error) {
	keyName := dbExplorer.DbTables.TablesMap[tableName].KeyName

	sqlStr := "DELETE FROM " +
		tableName +
		" WHERE " +
		keyName +
		" = ?"

	result, err := dbExplorer.Db.Exec(sqlStr, recordId)
	if err != nil {
		return 0, err
	}

	return result.RowsAffected()
}

type requestValue struct {
	Type  string
	Value interface{}
}

type request map[string]*requestValue

func parseRequest(r *http.Request) (*request, error) {
	defer func(b io.ReadCloser) {
		_ = b.Close()
	}(r.Body)
	var raw map[string]json.RawMessage
	err := json.NewDecoder(r.Body).Decode(&raw)
	if err != nil {
		return nil, err
	}

	res := request(make(map[string]*requestValue, len(raw)))
	for key, value := range raw {
		valueStr := string(value)
		i, err := strconv.ParseInt(valueStr, 10, 64)
		if err == nil {
			res[key] = &requestValue{"int", i}
			continue
		}
		f, err := strconv.ParseFloat(valueStr, 64)
		if err == nil {
			res[key] = &requestValue{"float", f}
			continue
		}

		var v *string
		err = json.Unmarshal(value, &v)
		if err != nil {
			return nil, err
		}
		if v == nil {
			res[key] = &requestValue{Value: nil}
		} else {
			res[key] = &requestValue{"string", *v}
		}
	}

	return &res, nil
}

type dbValue *string

type dbRecord []dbValue

type dbRecords []dbRecord

func dbQuery(db *sql.DB, sqlStr string, columnsCount, rowsCount int) (dbRecords, error) {
	rows, err := db.Query(sqlStr)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()

	res := dbRecords(make([]dbRecord, 0, rowsCount))

	var record dbRecord
	var row []interface{}
	for rows.Next() {
		record = make([]dbValue, columnsCount)
		row = make([]interface{}, columnsCount)
		for i := 0; i < columnsCount; i++ {
			row[i] = &record[i]
		}
		if err = rows.Scan(row...); err != nil {
			return nil, err
		}
		res = append(res, record)
	}

	return res, nil
}

func convertDbValue(value dbValue, column *dbColumn) (interface{}, error) {
	if value == nil {
		return nil, nil
	}

	switch column.Type {
	case "int":
		return strconv.Atoi(*value)
	case "float":
		return strconv.ParseFloat(*value, 64)
	default:
		return *value, nil
	}
}
