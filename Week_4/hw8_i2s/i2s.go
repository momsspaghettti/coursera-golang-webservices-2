package main

import (
	"fmt"
	"reflect"
)

func i2s(in interface{}, out interface{}) error {
	outVal := reflect.ValueOf(out)

	if outVal.Kind() != reflect.Ptr {
		return fmt.Errorf("expected out as ptr")
	}

	val := outVal.Elem()

	inType := reflect.TypeOf(in)
	inVal := reflect.ValueOf(in)

	switch val.Kind() {
	case reflect.Struct:
		if inType.Kind() != reflect.Map {
			return fmt.Errorf("out as struct expected in as map")
		}

		for i := 0; i < val.NumField(); i++ {
			field := val.Type().Field(i)
			fieldName := field.Name

			mapRef := inVal.MapIndex(reflect.ValueOf(fieldName))
			mapValue := mapRef.Elem()
			mapValueTypeStr := mapValue.Type().String()

			fieldObj := val.FieldByName(fieldName)

			switch fieldObj.Kind() {
			case reflect.Int:
				if mapValueTypeStr != "float64" {
					return fmt.Errorf("incompatible types")
				}
				fieldObj.Set(reflect.ValueOf(int(mapValue.Float())))

			case reflect.Float64:
				if mapValueTypeStr != "float64" {
					return fmt.Errorf("incompatible types")
				}
				fieldObj.Set(reflect.ValueOf(mapValue.Float()))

			case reflect.String:
				if mapValueTypeStr != "string" {
					return fmt.Errorf("incompatible types")
				}
				fieldObj.Set(reflect.ValueOf(mapValue.String()))

			case reflect.Bool:
				if mapValueTypeStr != "bool" {
					return fmt.Errorf("incompatible types")
				}
				fieldObj.Set(reflect.ValueOf(mapValue.Bool()))

			default:
				if err := i2s(mapValue.Interface(), fieldObj.Addr().Interface()); err != nil {
					return err
				}
			}
		}

	case reflect.Slice:
		if inType.Kind() != reflect.Slice {
			return fmt.Errorf("out as slice expected in as slice")
		}

		for i := 0; i < inVal.Len(); i++ {
			itemType := val.Type().Elem()
			newItem := reflect.New(itemType)
			if err := i2s(inVal.Index(i).Interface(), newItem.Interface()); err != nil {
				return err
			}
			val.Set(reflect.Append(val, newItem.Elem()))
		}
	}

	return nil
}
