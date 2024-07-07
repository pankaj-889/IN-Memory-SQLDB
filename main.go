package main

import (
	"errors"
	"fmt"
)

type ColumnType int

const (
	String ColumnType = iota
	Int
)

type Column struct {
	Name            string
	DataType        ColumnType
	MaxStringLength int
	MaxIntValue     int
}

type Table struct {
	Name    string
	Columns []Column
	Records []map[string]interface{}
}

type Database struct {
	Tables map[string]Table
}

func (db *Database) CreateTable(name string, columns []Column) error {
	if _, exists := db.Tables[name]; exists {
		return errors.New("table already exists")
	}
	db.Tables[name] = Table{
		Name:    name,
		Columns: columns,
		Records: []map[string]interface{}{},
	}
	return nil
}

func (db *Database) DeleteTable(name string) error {
	if _, exists := db.Tables[name]; !exists {
		return errors.New("table not exists")
	}
	delete(db.Tables, name)
	return nil
}

func (db *Database) AddRecord(name string, record map[string]interface{}) error {
	table, exists := db.Tables[name]
	if !exists {
		return errors.New("table already exists")
	}

	for _, col := range table.Columns {
		value, exists := record[col.Name]
		if !exists {
			continue
		}

		switch col.DataType {
		case String:
			strValue, ok := value.(string)
			if !ok {
				return fmt.Errorf("column %s expects a string value", col.Name)
			}
			if col.MaxStringLength > 0 && len(strValue) > col.MaxStringLength {
				return fmt.Errorf("column %s exceeds maximum length of %d", col.Name, col.MaxStringLength)
			}
		case Int:
			intValue, ok := value.(int)
			if !ok {
				return fmt.Errorf("column %s expects an int value", col.Name)
			}
			if intValue > col.MaxIntValue {
				return fmt.Errorf("column %s is below the minimum value of %d", col.Name, col.MaxIntValue)
			}
		}
	}
	table.Records = append(table.Records, record)
	db.Tables[name] = table
	return nil
}

func (db *Database) PrintRecords(name string) error {
	table, exists := db.Tables[name]
	if !exists {
		return errors.New("table not exists")
	}
	for _, record := range table.Records {
		fmt.Println(record)
	}
	return nil
}

func (db *Database) FilterRecord(tableName string, columnName string, value interface{}) error {
	table, exists := db.Tables[tableName]
	if !exists {
		return errors.New("table does not exist")
	}

	for _, record := range table.Records {
		if record[columnName] == value {
			fmt.Println(record)
		}
	}
	return nil
}

func (db *Database) UpdateTable(name string, columns []Column) error {
	table, exists := db.Tables[name]
	if !exists {
		return errors.New("table does not exist")
	}
	table.Columns = columns
	db.Tables[name] = table
	return nil
}

func main() {

	db := Database{Tables: make(map[string]Table)}

	columns := []Column{
		{Name: "id", DataType: Int, MaxStringLength: 1024, MaxIntValue: 20},
		{Name: "value", DataType: String, MaxStringLength: 1024, MaxIntValue: 20},
	}

	err := db.CreateTable("table1", columns)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	record := map[string]interface{}{
		"id":    1,
		"value": "hello",
	}
	err = db.AddRecord("table1", record)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	record = map[string]interface{}{
		"id":    2,
		"value": "world",
	}

	err = db.AddRecord("table1", record)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	fmt.Println("All records:")
	err = db.PrintRecords("table1")
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	newColumns := []Column{
		{Name: "id", DataType: Int, MaxStringLength: 1024, MaxIntValue: 20},
		{Name: "name", DataType: String, MaxStringLength: 1024, MaxIntValue: 20},
		{Name: "email", DataType: String, MaxStringLength: 1024, MaxIntValue: 20},
	}

	err = db.UpdateTable("users", newColumns)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	err = db.DeleteTable("users")
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	/*add record in the hello table
	first get the table
	check the record is existed in the TABLE or not using key
	column is string and int
	fetch the data from the map string value and check the length with MaxStringLength
	and same as with int value with MaxIntValue
	if the criteria satisfy
	if not present then add the record a string and int
	let say map[record] = {
		10,
		"hello world"


	*/

}
