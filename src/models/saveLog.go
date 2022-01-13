package models

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/ClickHouse/clickhouse-go"
	log "github.com/sirupsen/logrus"
	"push_service/src/config"
	"time"
)

type SaveLogModel struct {
}

func (SaveLogModel) SaveData(logsData [2]string) interface{} {
	connect, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000?debug=true")
	if err != nil {
		log.Info(err)
	}

	if err := connect.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			fmt.Printf("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		} else {
			fmt.Println(err)
		}
		return nil
	}

	tableName := time.Now().Format("2006_01_02") + "_apiLog"

	_, err = connect.Exec(`
		CREATE TABLE IF NOT EXISTS ` + tableName + ` (
			id              String,
			apiMethod        String,
			url              String,
			params     String,
			exception  String,
			httpCode   String,
			response   String,
			logLever  String,
			createAt  DateTime
		) engine=Memory
	`)

	if err != nil {
		log.Fatal(err)
	}

	var (
		tx, _   = connect.Begin()
		stmt, _ = tx.Prepare("INSERT INTO " + tableName + "  (id, apiMethod, url, params, exception, httpCode,response,logLever,createAt) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")
	)

	defer stmt.Close()

	for _, data := range logsData {
		var logs config.SaveLog
		err = json.Unmarshal([]byte(data), &logs)
		if err != nil {
			fmt.Println(err)
		}

		bytes, e := json.Marshal(logs.Params)

		fmt.Println(logs)

		if e != nil {
			fmt.Println("序列化失败")
			continue
		}

		jsonStr := string(bytes)
		fmt.Println(jsonStr)
		fmt.Println("-------")
		fmt.Println(logs.Response)
		fmt.Println(logs.Exception)
		fmt.Println(logs.HttpCode)
		if _, err := stmt.Exec(
			logs.Id,
			logs.ApiMethod,
			logs.Url,
			jsonStr,
			logs.Exception,
			logs.HttpCode,
			logs.Response,
			logs.LogLever,
			logs.CreateAt,
		); err != nil {
			log.Fatal(err)
		}
	}

	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}

	//rows, err := connect.Query("SELECT country_code, os_id, browser_id, categories, action_day, action_time FROM example")
	//if err != nil {
	//	log.Fatal(err)
	//}
	//defer rows.Close()
	//
	//for rows.Next() {
	//	var (
	//		country               string
	//		os, browser           uint8
	//		categories            []int16
	//		actionDay, actionTime time.Time
	//	)
	//	if err := rows.Scan(&country, &os, &browser, &categories, &actionDay, &actionTime); err != nil {
	//		log.Fatal(err)
	//	}
	//	log.Info("country: %s, os: %d, browser: %d, categories: %v, action_day: %s, action_time: %s", country, os, browser, categories, actionDay, actionTime)
	//}

	//if err := rows.Err(); err != nil {
	//	log.Fatal(err)
	//}

	return nil
}
