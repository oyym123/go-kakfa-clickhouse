package server

import (
	"fmt"
	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"net/http"
	"os"
	"time"
)

type Producer struct {
	ID string
}

func producerHandler(kafkaWriter *kafka.Writer) func(http.ResponseWriter, *http.Request) {
	return func(wrt http.ResponseWriter, req *http.Request) {
		t := time.Now()
		body, err := ioutil.ReadAll(req.Body)
		if err != nil {
			log.Fatalln(err)
		}

		msg := kafka.Message{
			Key:   []byte(fmt.Sprintf("address-%s", req.RemoteAddr)),
			Value: body,
		}

		err = kafkaWriter.WriteMessages(req.Context(), msg)
		elapsed := time.Since(t)
		fmt.Println("app elapsed----:", elapsed)

		if err != nil {
			_, _ = wrt.Write([]byte(err.Error()))
			log.Fatalln(err)
		}
	}
}

func getKafkaWriter(kafkaURL, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP(kafkaURL),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 10 * time.Nanosecond,
		ReadTimeout:  100 * time.Millisecond,
		WriteTimeout: 100 * time.Millisecond,
	}
}

func (Producer) Send(http.ResponseWriter, *http.Request) interface{} {
	// get kafka writer using environment variables.
	kafkaURL := "127.0.0.1:9092"
	topic := "topic1"
	chExit := make(chan os.Signal)
	go func() {
		mux := http.NewServeMux()
		kafkaWriter := getKafkaWriter(kafkaURL, topic)
		defer kafkaWriter.Close()
		// Add handle func for producer.
		mux.HandleFunc("/", producerHandler(kafkaWriter))
		// Run the web server.
		fmt.Println("start producer-api ... !!")
		log.Fatal(http.ListenAndServe("127.0.0.1:8098", mux))
	}()
	//主进程阻塞直到有退出信号
	s := <-chExit
	log.Info("Get signal:", s)
	return nil
}
