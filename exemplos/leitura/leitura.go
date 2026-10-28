package leitura

import (
	"encoding/json"
	"math/rand"
	"strings"
	"time"
)

type Leitura struct {
	Sensor      string  `json:"sensor"`
	Seq         int     `json:"seq"`
	Temperatura float64 `json:"temperatura"`
	Gerada      int64   `json:"gerada"`
}

func Nova(sensor string, seq int) Leitura {
	return Leitura{
		Sensor:      sensor,
		Seq:         seq,
		Temperatura: 24 + rand.Float64()*6,
		Gerada:      time.Now().UnixNano(),
	}
}

func (l Leitura) Bytes() []byte {
	b, _ := json.Marshal(l)
	return b
}

func Ler(dados []byte) (Leitura, error) {
	var l Leitura
	err := json.Unmarshal(dados, &l)
	return l, err
}

func Latencia(l Leitura) time.Duration {
	return time.Since(time.Unix(0, l.Gerada))
}

func ListaBrokers(s string) []string {
	var lista []string
	for _, b := range strings.Split(s, ",") {
		b = strings.TrimSpace(b)
		if b != "" {
			lista = append(lista, b)
		}
	}
	return lista
}
