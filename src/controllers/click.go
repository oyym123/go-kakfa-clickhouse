package controllers

import (
	"fmt"
	"net/http"
	"push_service/src/models"
)

type ClickController struct{
	BaseController
}

func (c *ClickController) SaveData(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		_, _ = fmt.Fprintf(w, "method not allowed!")
		return
	}

	// read request
	var search models.ClickHouseModel
	search.Init()
	res := search.SaveData(w, r)
	c.sendOk(w, res)
}
