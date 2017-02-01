package gorm

import (
	"crypto/sha1"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/ngorm/common"
	"github.com/ngorm/ngorm/model"
)

type Mysql struct {
	common.Dialect
}

func (Mysql) GetName() string {
	return "mysql"
}

func (Mysql) Quote(key string) string {
	return fmt.Sprintf("`%s`", key)
}

// Get Data Type for MySQL Dialect
func (Mysql) DataTypeOf(field *model.StructField) (string, error) {
	dataValue, sqlType, size, additionalType :=
		model.ParseFieldStructForDialect(field)

	// MySQL allows only one auto increment column per table, and it must
	// be a KEY column.
	if _, ok := field.TagSettings["AUTO_INCREMENT"]; ok {
		if _, ok = field.TagSettings["INDEX"]; !ok && !field.IsPrimaryKey {
			delete(field.TagSettings, "AUTO_INCREMENT")
		}
	}

	if sqlType == "" {
		switch dataValue.Kind() {
		case reflect.Bool:
			sqlType = "boolean"
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32:
			if _, ok := field.TagSettings["AUTO_INCREMENT"]; ok || field.IsPrimaryKey {
				field.TagSettings["AUTO_INCREMENT"] = "AUTO_INCREMENT"
				sqlType = "int AUTO_INCREMENT"
			} else {
				sqlType = "int"
			}
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uintptr:
			if _, ok := field.TagSettings["AUTO_INCREMENT"]; ok || field.IsPrimaryKey {
				field.TagSettings["AUTO_INCREMENT"] = "AUTO_INCREMENT"
				sqlType = "int unsigned AUTO_INCREMENT"
			} else {
				sqlType = "int unsigned"
			}
		case reflect.Int64:
			if _, ok := field.TagSettings["AUTO_INCREMENT"]; ok || field.IsPrimaryKey {
				field.TagSettings["AUTO_INCREMENT"] = "AUTO_INCREMENT"
				sqlType = "bigint AUTO_INCREMENT"
			} else {
				sqlType = "bigint"
			}
		case reflect.Uint64:
			if _, ok := field.TagSettings["AUTO_INCREMENT"]; ok || field.IsPrimaryKey {
				field.TagSettings["AUTO_INCREMENT"] = "AUTO_INCREMENT"
				sqlType = "bigint unsigned AUTO_INCREMENT"
			} else {
				sqlType = "bigint unsigned"
			}
		case reflect.Float32, reflect.Float64:
			sqlType = "double"
		case reflect.String:
			if size > 0 && size < 65532 {
				sqlType = fmt.Sprintf("varchar(%d)", size)
			} else {
				sqlType = "longtext"
			}
		case reflect.Struct:
			if _, ok := dataValue.Interface().(time.Time); ok {
				if _, ok := field.TagSettings["NOT NULL"]; ok {
					sqlType = "timestamp"
				} else {
					sqlType = "timestamp NULL"
				}
			}
		default:
			if _, ok := dataValue.Interface().([]byte); ok {
				if size > 0 && size < 65532 {
					sqlType = fmt.Sprintf("varbinary(%d)", size)
				} else {
					sqlType = "longblob"
				}
			}
		}
	}

	if sqlType == "" {
		return "", fmt.Errorf("invalid sql type %s (%s) for mysql",
			dataValue.Type().Name(), dataValue.Kind().String())
	}

	if strings.TrimSpace(additionalType) == "" {
		return sqlType, nil
	}
	return fmt.Sprintf("%v %v", sqlType, additionalType), nil
}

func (s Mysql) RemoveIndex(tableName string, indexName string) error {
	_, err := s.DB.Exec(fmt.Sprintf("DROP INDEX %v ON %v",
		indexName, s.Quote(tableName)))
	return err
}

func (s Mysql) HasForeignKey(tableName string, foreignKeyName string) bool {
	var count int
	query := `
SELECT Count(*)
FROM   information_schema.table_constraints
WHERE  constraint_schema = ?
       AND table_name = ?
       AND constraint_name = ?
       AND constraint_type = 'FOREIGN KEY'
	`
	s.DB.QueryRow(query, s.CurrentDatabase(),
		tableName, foreignKeyName).Scan(&count)
	return count > 0
}

func (s Mysql) CurrentDatabase() (name string) {
	s.DB.QueryRow("SELECT DATABASE()").Scan(&name)
	return
}

func (Mysql) SelectFromDummyTable() string {
	return "FROM DUAL"
}

func (s Mysql) BuildForeignKeyName(tableName, field, dest string) string {
	keyName := s.Dialect.BuildForeignKeyName(tableName, field, dest)
	if utf8.RuneCountInString(keyName) <= 64 {
		return keyName
	}
	h := sha1.New()
	h.Write([]byte(keyName))
	bs := h.Sum(nil)

	// sha1 is 40 digits, keep first 24 characters of destination
	destRunes := []rune(regexp.MustCompile("(_*[^a-zA-Z]+_*|_+)").ReplaceAllString(dest, "_"))
	if len(destRunes) > 24 {
		destRunes = destRunes[:24]
	}

	return fmt.Sprintf("%s%x", string(destRunes), bs)
}
