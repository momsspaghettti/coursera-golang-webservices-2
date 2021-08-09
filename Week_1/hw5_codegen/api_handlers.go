package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

// auto-generated file: do not edit!

type httpResult struct {
	Error    string      `json:"error"`
	Response interface{} `json:"response"`
}

func marshal(res httpResult) []byte {
	resMap := make(map[string]interface{})
	resMap["error"] = res.Error
	if res.Response != nil {
		resMap["response"] = res.Response
	}
	resultStr, _ := json.Marshal(resMap)
	return resultStr
}

func writeResponse(w http.ResponseWriter, response []byte) {
	_, _ = w.Write(response)
}

func contains(arr []string, item string) bool {
	for _, i := range arr {
		if item == i {
			return true
		}
	}
	return false
}

func printSlice(s []string) string {
	return "[" + strings.Join(s, ", ") + "]"
}

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
}

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
}

func (h *OtherApi) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/user/create":
		h.wrapperCreate(w, r)
	default:
		w.WriteHeader(http.StatusNotFound)
		writeResponse(w, marshal(httpResult{Error: "unknown method"}))
	}
}

func (h *OtherApi) wrapperCreate(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	if r.Method != "POST" {
		w.WriteHeader(http.StatusNotAcceptable)
		writeResponse(w, marshal(httpResult{Error: "bad method"}))
		return
	}

	if r.Header.Get("X-Auth") != "100500" {
		w.WriteHeader(http.StatusForbidden)
		writeResponse(w, marshal(httpResult{Error: "unauthorized"}))
		return
	}

	p0, err := validateAndBuildOtherCreateParams(r)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		writeResponse(w, marshal(httpResult{Error: err.Error()}))
		return
	}

	res, err := h.Create(
		ctx,
		*p0,
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

func (h *MyApi) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/user/create":
		h.wrapperCreate(w, r)
	case "/user/profile":
		h.wrapperProfile(w, r)
	default:
		w.WriteHeader(http.StatusNotFound)
		writeResponse(w, marshal(httpResult{Error: "unknown method"}))
	}
}

func (h *MyApi) wrapperProfile(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	p0, err := validateAndBuildProfileParams(r)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		writeResponse(w, marshal(httpResult{Error: err.Error()}))
		return
	}

	res, err := h.Profile(
		ctx,
		*p0,
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

func (h *MyApi) wrapperCreate(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	if r.Method != "POST" {
		w.WriteHeader(http.StatusNotAcceptable)
		writeResponse(w, marshal(httpResult{Error: "bad method"}))
		return
	}

	if r.Header.Get("X-Auth") != "100500" {
		w.WriteHeader(http.StatusForbidden)
		writeResponse(w, marshal(httpResult{Error: "unauthorized"}))
		return
	}

	p0, err := validateAndBuildCreateParams(r)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		writeResponse(w, marshal(httpResult{Error: err.Error()}))
		return
	}

	res, err := h.Create(
		ctx,
		*p0,
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

func validateAndBuildCreateParams(r *http.Request) (*CreateParams, error) {
	res := CreateParams{}

	var paramName string
	var paramValue string
	var required bool
	var defaultValue string
	var enum []string

	var err error

	paramName = strings.ToLower("Age")

	paramValue = r.FormValue(paramName)
	required = false

	if required && paramValue == "" {
		return nil, fmt.Errorf(paramName + " must me not empty")
	}

	defaultValue = ""
	if paramValue == "" && defaultValue != "" {
		paramValue = defaultValue
	}

	enum = make([]string, 0)
	if len(enum) > 0 && !contains(enum, paramValue) {
		return nil, fmt.Errorf(paramName + " must be one of " + printSlice(enum))
	}

	intAgeVal, err := strconv.Atoi(paramValue)
	if err != nil {
		return nil, fmt.Errorf(paramName + " must be int")
	}
	if err = validateMinMaxInt(intAgeVal, paramName, "0", "128"); err != nil {
		return nil, err
	}
	res.Age = intAgeVal

	paramName = strings.ToLower("Login")

	paramValue = r.FormValue(paramName)
	required = true

	if required && paramValue == "" {
		return nil, fmt.Errorf(paramName + " must me not empty")
	}

	defaultValue = ""
	if paramValue == "" && defaultValue != "" {
		paramValue = defaultValue
	}

	enum = make([]string, 0)
	if len(enum) > 0 && !contains(enum, paramValue) {
		return nil, fmt.Errorf(paramName + " must be one of " + printSlice(enum))
	}

	if err = validateMinMaxStr(paramValue, paramName, "10", ""); err != nil {
		return nil, err
	}
	res.Login = paramValue

	paramName = "full_name"

	paramValue = r.FormValue(paramName)
	required = false

	if required && paramValue == "" {
		return nil, fmt.Errorf(paramName + " must me not empty")
	}

	defaultValue = ""
	if paramValue == "" && defaultValue != "" {
		paramValue = defaultValue
	}

	enum = make([]string, 0)
	if len(enum) > 0 && !contains(enum, paramValue) {
		return nil, fmt.Errorf(paramName + " must be one of " + printSlice(enum))
	}

	if err = validateMinMaxStr(paramValue, paramName, "", ""); err != nil {
		return nil, err
	}
	res.Name = paramValue

	paramName = strings.ToLower("Status")

	paramValue = r.FormValue(paramName)
	required = false

	if required && paramValue == "" {
		return nil, fmt.Errorf(paramName + " must me not empty")
	}

	defaultValue = "user"
	if paramValue == "" && defaultValue != "" {
		paramValue = defaultValue
	}

	enum = make([]string, 0)
	enum = append(enum, "user")
	enum = append(enum, "moderator")
	enum = append(enum, "admin")
	if len(enum) > 0 && !contains(enum, paramValue) {
		return nil, fmt.Errorf(paramName + " must be one of " + printSlice(enum))
	}

	if err = validateMinMaxStr(paramValue, paramName, "", ""); err != nil {
		return nil, err
	}
	res.Status = paramValue

	return &res, nil
}

func validateAndBuildOtherCreateParams(r *http.Request) (*OtherCreateParams, error) {
	res := OtherCreateParams{}

	var paramName string
	var paramValue string
	var required bool
	var defaultValue string
	var enum []string

	var err error

	paramName = strings.ToLower("Class")

	paramValue = r.FormValue(paramName)
	required = false

	if required && paramValue == "" {
		return nil, fmt.Errorf(paramName + " must me not empty")
	}

	defaultValue = "warrior"
	if paramValue == "" && defaultValue != "" {
		paramValue = defaultValue
	}

	enum = make([]string, 0)
	enum = append(enum, "warrior")
	enum = append(enum, "sorcerer")
	enum = append(enum, "rouge")
	if len(enum) > 0 && !contains(enum, paramValue) {
		return nil, fmt.Errorf(paramName + " must be one of " + printSlice(enum))
	}

	if err = validateMinMaxStr(paramValue, paramName, "", ""); err != nil {
		return nil, err
	}
	res.Class = paramValue

	paramName = strings.ToLower("Level")

	paramValue = r.FormValue(paramName)
	required = false

	if required && paramValue == "" {
		return nil, fmt.Errorf(paramName + " must me not empty")
	}

	defaultValue = ""
	if paramValue == "" && defaultValue != "" {
		paramValue = defaultValue
	}

	enum = make([]string, 0)
	if len(enum) > 0 && !contains(enum, paramValue) {
		return nil, fmt.Errorf(paramName + " must be one of " + printSlice(enum))
	}

	intLevelVal, err := strconv.Atoi(paramValue)
	if err != nil {
		return nil, fmt.Errorf(paramName + " must be int")
	}
	if err = validateMinMaxInt(intLevelVal, paramName, "1", "50"); err != nil {
		return nil, err
	}
	res.Level = intLevelVal

	paramName = "account_name"

	paramValue = r.FormValue(paramName)
	required = false

	if required && paramValue == "" {
		return nil, fmt.Errorf(paramName + " must me not empty")
	}

	defaultValue = ""
	if paramValue == "" && defaultValue != "" {
		paramValue = defaultValue
	}

	enum = make([]string, 0)
	if len(enum) > 0 && !contains(enum, paramValue) {
		return nil, fmt.Errorf(paramName + " must be one of " + printSlice(enum))
	}

	if err = validateMinMaxStr(paramValue, paramName, "", ""); err != nil {
		return nil, err
	}
	res.Name = paramValue

	paramName = strings.ToLower("Username")

	paramValue = r.FormValue(paramName)
	required = true

	if required && paramValue == "" {
		return nil, fmt.Errorf(paramName + " must me not empty")
	}

	defaultValue = ""
	if paramValue == "" && defaultValue != "" {
		paramValue = defaultValue
	}

	enum = make([]string, 0)
	if len(enum) > 0 && !contains(enum, paramValue) {
		return nil, fmt.Errorf(paramName + " must be one of " + printSlice(enum))
	}

	if err = validateMinMaxStr(paramValue, paramName, "3", ""); err != nil {
		return nil, err
	}
	res.Username = paramValue

	return &res, nil
}

func validateAndBuildProfileParams(r *http.Request) (*ProfileParams, error) {
	res := ProfileParams{}

	var paramName string
	var paramValue string
	var required bool
	var defaultValue string
	var enum []string

	var err error

	paramName = strings.ToLower("Login")

	paramValue = r.FormValue(paramName)
	required = true

	if required && paramValue == "" {
		return nil, fmt.Errorf(paramName + " must me not empty")
	}

	defaultValue = ""
	if paramValue == "" && defaultValue != "" {
		paramValue = defaultValue
	}

	enum = make([]string, 0)
	if len(enum) > 0 && !contains(enum, paramValue) {
		return nil, fmt.Errorf(paramName + " must be one of " + printSlice(enum))
	}

	if err = validateMinMaxStr(paramValue, paramName, "", ""); err != nil {
		return nil, err
	}
	res.Login = paramValue

	return &res, nil
}
