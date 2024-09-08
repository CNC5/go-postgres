package main

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	//"golang.org/x/crypto/argon2"
	//"net/http"
)

// Standard functions
func concatenate(a, b string) string {
	result := ""
	if len(a) == 0 {
		result = b
	} else {
		result = fmt.Sprintf("%s %s", a, b)
	}
	return result
}

// database structures
type database struct {
	address      string
	databaseName string
	user         string
	password     string
	connection   *pgxpool.Pool
	tables       map[string]databaseTable
}
type databaseTable struct {
	columns map[string]databaseTableColumn
}
type databaseTableColumn struct {
	columnType  databaseTableColumnType
	constraints databaseTableColumnConstraints
}
type databaseTableColumnType struct {
	columnType reflect.Kind
	size       int
}
type databaseTableColumnConstraints struct {
	notNull    bool
	unique     bool
	primaryKey bool
	check      bool
	foreignKey bool
}

// Database methods, ActionObjectSpec

func (column databaseTableColumn) asString() (string, error) {
	data := fmt.Sprintf("Column of type %s and size %d", column.columnType.columnType, column.columnType.size)
	return data, nil
}
func NewDatabase(address, dbname, dbuser, dbpassword string) database {
	newDB := database{address: address, databaseName: dbname, user: dbuser, password: dbpassword}
	newDB.tables = make(map[string]databaseTable)
	return newDB
}
func mapTypeToPGTypeString(variableType reflect.Kind) (string, error) {
	databaseTypesMap := map[reflect.Kind]string{
		reflect.String:  "VARCHAR",
		reflect.Int8:    "SMALLINT",
		reflect.Int16:   "SMALLINT",
		reflect.Int32:   "INTEGER",
		reflect.Int64:   "BIGINT",
		reflect.Int:     "BIGINT",
		reflect.Float32: "FLOAT4",
		reflect.Float64: "FLOAT8",
		reflect.Bool:    "BOOLEAN",
	}
	pgTypeString, doesExist := databaseTypesMap[variableType]
	if doesExist {
		return pgTypeString, nil
	} else {
		return "", errors.New("no such type")
	}
}
func (db *database) Connect() error {
	connectionString := fmt.Sprintf("postgres://%s:%s@%s/%s", db.user, db.password, db.address, db.databaseName)
	dbpool, err := pgxpool.New(context.Background(), connectionString)
	if err != nil {
		return err
	}
	db.connection = dbpool
	return nil
}
func (db *database) AddTable(tableName string, table databaseTable) error {
	var columnsSlice []string
	for name, column := range table.columns {
		constraints := column.constraints
		constraintsString := ""
		if constraints.notNull {
			constraintsString = concatenate(constraintsString, "NOT NULL")
		}
		if constraints.check {
			constraintsString = concatenate(constraintsString, "CHECK")
		}
		if constraints.foreignKey {
			constraintsString = concatenate(constraintsString, "FOREIGN KEY")
		}
		if constraints.unique {
			constraintsString = concatenate(constraintsString, "UNIQUE")
		}
		if constraints.primaryKey {
			constraintsString = concatenate(constraintsString, "PRIMARY KEY")
		}
		variableTypeString, err := mapTypeToPGTypeString(column.columnType.columnType)
		if err != nil {
			fmt.Println(err)
		}
		columnsSlice = append(columnsSlice, fmt.Sprintf("%s %s(%d) %s", name, variableTypeString, column.columnType.size, constraintsString))
	}
	queryString := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s);", tableName, strings.Join(columnsSlice, ", "))
	if db.connection != nil {
		_, err := db.connection.Exec(context.Background(), queryString)
		if err != nil {
			return err
		} else {
			db.tables[tableName] = table
			return nil
		}
	} else {
		return errors.New("database is not connected")
	}
}
func (db *database) CreateAllTables() {
	for name, table := range db.tables {
		err := db.AddTable(name, table)
		if err != nil {
			fmt.Println(err)
		}
	}
}
func (db *database) DeleteTable(table string) error {
	_, err := db.connection.Exec(context.Background(), fmt.Sprintf("DROP TABLE %s;", table))
	return err
}
func (db *database) InsertRow(tableName string, values map[string]any) error {
	table, doesExist := db.tables[tableName]
	if !doesExist {
		return errors.New("table requested for insertion does not exist in the data model")
	}
	var insertKeys []string
	var insertValues []string
	for key, value := range values {
		column, doesExist := table.columns[key]
		if !doesExist {
			return errors.New("column requested for insertion does not exist in the data model")
		}
		insertKeys = append(insertKeys, key)
		// type-check
		valueType := reflect.TypeOf(value).Kind()
		if column.columnType.columnType != valueType {
			return fmt.Errorf("value type requested for insertion is incorrect, tried to insert %v value into %v type column", reflect.TypeOf(value), column.columnType.columnType)
		}
		intTypes := map[reflect.Kind]bool{reflect.Int: true, reflect.Int32: true, reflect.Int64: true}
		floatTypes := map[reflect.Kind]bool{reflect.Float32: true, reflect.Float64: true}
		stringValue := ""
		if _, isInt := intTypes[valueType]; isInt {
			stringValue = strconv.FormatInt(value.(int64), 10)
		} else if _, isFloat := floatTypes[valueType]; isFloat {
			stringValue = strconv.FormatFloat(value.(float64), 'e', 20, 64)
		} else if valueType == reflect.String {
			stringValue = fmt.Sprintf("'%s'", value.(string))
		} else if valueType == reflect.Bool {
			stringValue = strconv.FormatBool(value.(bool))
		} else {
			return errors.New("value did not match any type")
		}
		insertValues = append(insertValues, stringValue)
	}
	insertString := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);", tableName, strings.Join(insertKeys, ","), strings.Join(insertValues, ","))
	_, err := db.connection.Exec(context.Background(), insertString)
	if err != nil {
		return err
	}
	return nil
}

func main() {
	myDB := NewDatabase("localhost:5432", "test", "test_admin", "1234")
	usersTable := databaseTable{
		columns: map[string]databaseTableColumn{
			"id": {
				columnType:  databaseTableColumnType{columnType: reflect.String, size: 255},
				constraints: databaseTableColumnConstraints{primaryKey: true},
			},
			"username": {
				columnType:  databaseTableColumnType{columnType: reflect.String, size: 255},
				constraints: databaseTableColumnConstraints{notNull: true, unique: true},
			},
			"password": {
				columnType:  databaseTableColumnType{columnType: reflect.String, size: 255},
				constraints: databaseTableColumnConstraints{notNull: true},
			},
		},
	}
	err := myDB.Connect()
	if err != nil {
		fmt.Println(err)
	}
	err = myDB.AddTable("users", usersTable)
	if err != nil {
		fmt.Println(err)
	}
	err = myDB.InsertRow("users", map[string]any{"id": "2n1kj", "username": "John", "password": "1234"})
	if err != nil {
		fmt.Println(err)
	}
	fmt.Print(usersTable.columns["id"].asString())
}
