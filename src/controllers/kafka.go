package controllers

import (
	"fmt"
	"net/http"
	"push_service/src/server"
)

type KafkaController struct {
	BaseController
}

func (c *KafkaController) Get(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		_, _ = fmt.Fprintf(w, "method not allowed!")
		return
	}

	// read request
	var consumer server.Consumer
	res := consumer.Get(w, r)
	c.sendOk(w, res)
}

func (c *KafkaController) Send(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		_, _ = fmt.Fprintf(w, "method not allowed!")
		return
	}

	// read request
	var producer server.Producer
	res := producer.Send(w, r)
	c.sendOk(w, res)
}
