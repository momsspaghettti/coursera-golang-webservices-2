package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/printer"
	"go/token"
	"io"
	"log"
	"os"
	"reflect"
	"strings"
	"text/template"
)

// код писать тут

var serveHTTPMethodTpl = template.Must(template.New("serveHTTPMethodTpl").Parse(`
func (h *{{.Name}}) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	{{- range $key, $value := .Methods -}}
	case "{{$value.Specs.Url}}":
		h.wrapper{{ $value.Name }}(w, r)
	{{end -}}
	default:
		w.WriteHeader(http.StatusNotFound)
		writeResponse(w, marshal(httpResult{Error: "unknown method"}))
	}
}
`))

var handlerMethodTpl = template.Must(template.New("handlerMethodTpl").Parse(`
func (h *{{.ObjectName}}) wrapper{{.Name}}(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	{{if ne .Specs.Method ""}}
	if r.Method != "{{.Specs.Method}}" {
		w.WriteHeader(http.StatusNotAcceptable)
		writeResponse(w, marshal(httpResult{Error: "bad method"}))
		return
	}
	{{end}}

	{{if .Specs.Auth}}
	if r.Header.Get("X-Auth") != "100500" {
		w.WriteHeader(http.StatusForbidden)
		writeResponse(w, marshal(httpResult{Error: "unauthorized"}))
		return
	}
	{{end}}

	{{- range $i, $pType := .ParamTypes}}
	p{{$i}}, err := validateAndBuild{{$pType}}(r)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		writeResponse(w, marshal(httpResult{Error: err.Error()}))
		return
	}
	{{end}}
	res, err := h.{{.Name}}(
		ctx,
		{{- range $i, $pType := .ParamTypes}}
		*p{{$i}},
		{{end}}
	)

	if err != nil {
		apiErr, ok := err.(ApiError)
		if ok {
			w.WriteHeader(apiErr.HTTPStatus)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
		}
		writeResponse(w, marshal(httpResult{Error: err.Error()}))
		return
	}

	w.WriteHeader(http.StatusOK)
	writeResponse(w, marshal(httpResult{Response: res}))
}
`))

var validateAndBuildDataStructTpl = template.Must(template.New("validateAndBuildDataStructTpl").Parse(`
func validateAndBuild{{.Name}}(r *http.Request) (*{{.Name}}, error) {
	res := {{.Name}}{}
	
	var paramName string
	var paramValue string
	var required bool
	var defaultValue string
	var enum []string

	var err error

	{{- range $key, $value := .Fields}}
	{{if ne $value.Validator.ParamName ""}}
	paramName = "{{$value.Validator.ParamName}}"
	{{else}}
	paramName = strings.ToLower("{{$key}}")
	{{end}}

	paramValue = r.FormValue(paramName)
	required = {{$value.Validator.Required}}
	
	if required && paramValue == "" {
		return nil, fmt.Errorf(paramName + " must me not empty")
	}

	defaultValue = "{{$value.Validator.Default}}"
	if paramValue == "" && defaultValue != "" {
		paramValue = defaultValue
	}

	enum = make([]string, 0)
	{{- range $value.Validator.Enum}}
	enum = append(enum, "{{.}}")
	{{- end}}
	if len(enum) > 0 && !contains(enum, paramValue) {
		return nil, fmt.Errorf(paramName + " must be one of " + printSlice(enum))
	}

	{{if eq $value.Type 0}}
	int{{$key}}Val, err := strconv.Atoi(paramValue)
	if err != nil {
		return nil, fmt.Errorf(paramName + " must be int")
	}
	if err = validateMinMaxInt(int{{$key}}Val, paramName, "{{$value.Validator.Min}}", "{{$value.Validator.Max}}"); err != nil {
		return nil, err
	}
	res.{{$key}} = int{{$key}}Val
	{{else}}
	if err = validateMinMaxStr(paramValue, paramName, "{{$value.Validator.Min}}", "{{$value.Validator.Max}}"); err != nil {
		return nil, err
	}
	res.{{$key}} = paramValue
	{{end}}
	{{end}}
	return &res, nil
}
`))

func main() {
	fSet := token.NewFileSet()
	root, err := parser.ParseFile(fSet, os.Args[1], nil, parser.ParseComments)
	checkAndLogError(err)

	var file *os.File
	file, err = os.Create(os.Args[2])
	checkAndLogError(err)

	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			log.Fatal(closeErr)
		}
	}()

	var out bytes.Buffer
	fPrintln(&out, `package `+root.Name.Name)

	var handlers *handlerObjects
	var structs *dataStructs
	handlers, structs, err = parse(root, fSet)
	checkAndLogError(err)

	err = generateCode(handlers, structs, &out)
	checkAndLogError(err)

	formattedCode, err := format.Source(out.Bytes())
	checkAndLogError(err)

	_, err = file.Write(formattedCode)
	checkAndLogError(err)
}

func checkAndLogError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func generateCode(handlers *handlerObjects, structs *dataStructs, w io.Writer) error {
	generateImports(w)
	generateCommon(w)

	for _, handler := range *handlers {
		if err := generateHandler(handler, w); err != nil {
			return err
		}
	}

	for _, s := range *structs {
		if err := validateAndBuildDataStructTpl.Execute(w, s); err != nil {
			return err
		}
	}

	return nil
}

func generateCommon(w io.Writer) {
	fPrintln(
		w,
		"type httpResult struct {\n\tError    string      `json:\"error\"`\n\tResponse interface{} `json:\"response\"`\n}",
	)

	fPrintln(w, `
func marshal(res httpResult) []byte {
	resMap := make(map[string]interface{})
	resMap["error"] = res.Error
	if res.Response != nil {
		resMap["response"] = res.Response
	}
	resultStr, _ := json.Marshal(resMap)
	return resultStr
}`)

	fPrintln(w, `
func writeResponse(w http.ResponseWriter, response []byte) {
	_, _ = w.Write(response)
}`)

	fPrintln(w, `
func contains(arr []string, item string) bool {
	for _, i := range arr {
		if item == i {
			return true
		}
	}
	return false
}`)

	fPrintln(w, `
func printSlice(s []string) string {
	return "[" + strings.Join(s, ", ") + "]"
}`)

	fPrintln(w, `
func validateMinMaxInt(value int, valueName, min, max string) error {
	if min != "" {
		minInt, err := strconv.Atoi(min)
		if err != nil {
			return err
		}
		if value < minInt {
			return fmt.Errorf(valueName + " must be >= " + min)
		}
	}

	if max != "" {
		maxInt, err := strconv.Atoi(max)
		if err != nil {
			return err
		}
		if value > maxInt {
			return fmt.Errorf(valueName + " must be <= " + max)
		}
	}

	return nil
}`)

	fPrintln(w, `
func validateMinMaxStr(value, valueName, min, max string) error {
	if min != "" {
		minInt, err := strconv.Atoi(min)
		if err != nil {
			return err
		}
		if len(value) < minInt {
			return fmt.Errorf(valueName + " len must be >= " + min)
		}
	}

	if max != "" {
		maxInt, err := strconv.Atoi(max)
		if err != nil {
			return err
		}
		if len(value) > maxInt {
			return fmt.Errorf(valueName + " len must be <= " + max)
		}
	}

	return nil
}`)
}

func generateImports(w io.Writer) {
	fPrintln(w, `
import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"fmt"
	"strconv"
)

// auto-generated file: do not edit!
`,
	)
}

func generateHandler(handler *handlerObject, w io.Writer) error {
	err := serveHTTPMethodTpl.Execute(w, handler)
	if err != nil {
		return err
	}

	for _, method := range *handler.Methods {
		if err = handlerMethodTpl.Execute(w, method); err != nil {
			return err
		}
	}

	return nil
}

func parse(root *ast.File, fSet *token.FileSet) (*handlerObjects, *dataStructs, error) {
	handlers := handlerObjects(make(map[string]*handlerObject))
	structs := dataStructs(make(map[string]*dataStruct))

	for _, node := range root.Decls {
		funcNode, isFuncNode := node.(*ast.FuncDecl)
		genNode, isGenNode := node.(*ast.GenDecl)

		if isFuncNode {
			if err := tryParseHandler(funcNode, &handlers, fSet); err != nil {
				return nil, nil, err
			}
			continue
		}

		if isGenNode {
			if err := tryParseDataStruct(genNode, &structs); err != nil {
				return nil, nil, err
			}
			continue
		}
	}

	return &handlers, &structs, nil
}

func tryParseHandler(funcNode *ast.FuncDecl, handlers *handlerObjects, fSet *token.FileSet) error {
	if funcNode.Doc == nil || len(funcNode.Doc.List) == 0 {
		return nil
	}

	apiGenJsonStr := ""
	for _, comment := range funcNode.Doc.List {
		if index := strings.Index(comment.Text, "apigen:api "); index != -1 {
			index += 11
			if index+2 >= len(comment.Text) {
				return fmt.Errorf("invalid handler func comment: %s", comment.Text)
			}
			apiGenJsonStr = comment.Text[index:]
			break
		}
	}

	if apiGenJsonStr == "" {
		return nil
	}

	handlerMethodSpecs := &HandlerMethodSpecs{}
	err := json.Unmarshal([]byte(apiGenJsonStr), handlerMethodSpecs)
	if err != nil {
		return err
	}

	// Объект метода
	objs := funcNode.Recv

	if objs == nil || len(objs.List) != 1 {
		return nil
	}

	var objectName string
	if obj, ok := objs.List[0].Type.(*ast.StarExpr); ok {
		objectName = obj.X.(*ast.Ident).Name
	}

	if objectName == "" {
		return nil
	}

	if _, exists := (*handlers)[objectName]; !exists {
		methods := handlerMethods(make(map[string]*handlerMethod))
		(*handlers)[objectName] = &handlerObject{objectName, &methods}
	}

	methodName := funcNode.Name.Name
	paramTypes := make([]string, 0, len(funcNode.Type.Params.List))

	// Первый параметры контекст, его пропускаем
	for _, param := range funcNode.Type.Params.List[1:] {
		var typeNameBuf bytes.Buffer
		err := printer.Fprint(&typeNameBuf, fSet, param.Type)
		if err != nil {
			return err
		}
		paramTypes = append(paramTypes, typeNameBuf.String())
	}

	(*(*handlers)[objectName].Methods)[methodName] = &handlerMethod{
		methodName,
		objectName,
		handlerMethodSpecs,
		paramTypes,
	}

	return nil
}

func tryParseDataStruct(genNode *ast.GenDecl, structs *dataStructs) error {
	for _, spec := range genNode.Specs {
		currType, ok := spec.(*ast.TypeSpec)
		if !ok {
			return nil
		}

		currStruct, ok := currType.Type.(*ast.StructType)
		if !ok {
			return nil
		}

		structName := currType.Name.Name

		hasFields := false
		fields := dataStructFields(make(map[string]*dataStructField))

	FieldsLoop:
		for _, fieldNode := range currStruct.Fields.List {
			if fieldNode.Tag == nil {
				continue FieldsLoop
			}
			tag := reflect.StructTag(fieldNode.Tag.Value[1 : len(fieldNode.Tag.Value)-1])
			var tagValue string
			if tagValue, ok = tag.Lookup("apivalidator"); !ok {
				continue FieldsLoop
			}

			validator, err := parseApiValidatorTagValue(tagValue)
			if err != nil {
				return err
			}

			fieldName := fieldNode.Names[0].Name
			fileType := fieldNode.Type.(*ast.Ident).Name
			var fieldTypeEnum FieldTypeEnum
			switch fileType {
			case "int":
				fieldTypeEnum = Int
			case "string":
				fieldTypeEnum = String
			default:
				return fmt.Errorf("invalid filed type: %s", fileType)
			}

			hasFields = true
			fields[fieldName] = &dataStructField{
				fieldName,
				fieldTypeEnum,
				validator,
			}
		}

		if hasFields {
			(*structs)[structName] = &dataStruct{
				structName,
				&fields,
			}
		}
	}

	return nil
}

func parseApiValidatorTagValue(tagValue string) (*apiValidator, error) {
	if len(tagValue) == 0 {
		return nil, fmt.Errorf("empty tagValue")
	}
	dict := strings.Split(tagValue, ",")
	res := apiValidator{}
	for _, kv := range dict {
		if len(kv) == 0 {
			return nil, fmt.Errorf("empty tagValue kv")
		}
		if strings.Contains(kv, "required") {
			res.Required = true
			continue
		}

		kvArr := strings.Split(kv, "=")
		if len(kvArr) != 2 || len(kvArr[0]) == 0 || len(kvArr[1]) == 0 {
			return nil, fmt.Errorf("invalid tagValue kv: %s", kv)
		}

		key := kvArr[0]
		value := kvArr[1]

		switch key {
		case "paramname":
			res.ParamName = value
		case "enum":
			res.Enum = strings.Split(value, "|")
		case "default":
			res.Default = value
		case "min":
			res.Min = value
		case "max":
			res.Max = value
		default:
			return nil, fmt.Errorf("unexpected tagValue key: %s", key)
		}
	}

	return &res, nil
}

type handlerObjects map[string]*handlerObject

type handlerObject struct {
	Name    string
	Methods *handlerMethods
}

type handlerMethods map[string]*handlerMethod

// Метод структуры обработчика
type handlerMethod struct {
	Name       string
	ObjectName string
	Specs      *HandlerMethodSpecs
	ParamTypes []string
}

type HandlerMethodSpecs struct {
	Url    string
	Auth   bool
	Method string
}

type dataStructs map[string]*dataStruct

// Описание структуры с тегом apivalidator
type dataStruct struct {
	Name   string
	Fields *dataStructFields
}

// Набор полей структуры
type dataStructFields map[string]*dataStructField

// Поле структуры
type dataStructField struct {
	Name      string
	Type      FieldTypeEnum
	Validator *apiValidator
}

type FieldTypeEnum int

const (
	Int FieldTypeEnum = iota
	String
)

type apiValidator struct {
	Required  bool
	ParamName string
	Enum      []string
	Default   string
	Min       string
	Max       string
}

func fPrintln(w io.Writer, p ...interface{}) {
	_, err := fmt.Fprintln(w, p...)
	checkAndLogError(err)
}
