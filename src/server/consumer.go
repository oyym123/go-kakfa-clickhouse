package server

import (
	"context"
	"github.com/segmentio/kafka-go"
	"log"
	"net/http"
	"push_service/src/models"
	"time"
)

type Consumer struct {
	ID string
}

func (Consumer) Get(w http.ResponseWriter, r *http.Request) interface{} {
	// make a new reader that consumes from topic-A, partition 0, at offset 42
	r1 := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{"localhost:9092"},
		Topic:     "topic1",
		Partition: 0,
		MinBytes:  10e3,            // 10KB
		MaxBytes:  100e6,           // 10MB
		MaxWait:   1 * time.Second, // 1s
	})

	_ = r1.SetOffsetAt(context.Background(), time.Now())

	//每次插入 clickhouse 的数量
	var saveLog models.SaveLogModel
	num := 0
	limit := 2
	var data [2]string
	var dataInit [2]string

	for {
		m, err := r1.ReadMessage(context.Background())
		if err != nil {
			break
		}
		data[num] = string(m.Value)
		//fmt.Printf("message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))
		num++
		if num%limit == 0 {
			saveLog.SaveData(data)
			data = dataInit
			num = 0
		}
	}

	if err := r1.Close(); err != nil {
		log.Fatal("failed to close reader:", err)
	}
	return nil
}
