package main

import (
	"flag"
	"log"
	"strings"
	"time"

	"github.com/FelipeBritoLC/middleware-pubsub/broker"
)

func main() {
	endereco := flag.String("endereco", ":7001", "endereco onde o broker vai escutar")
	pares := flag.String("pares", "", "enderecos dos outros brokers separados por virgula")
	buffer := flag.Int("buffer", 1000, "tamanho da fila de saida de cada cliente")
	maxPendentes := flag.Int("max-pendentes", 50, "maximo de publicacoes em processamento por conexao")
	timeoutEntrega := flag.Duration("timeout-entrega", 5*time.Second, "tempo maximo esperando o ACK de um inscrito")
	timeoutPar := flag.Duration("timeout-par", 8*time.Second, "tempo maximo esperando a resposta de outro broker")
	flag.Parse()

	var listaPares []string
	for _, p := range strings.Split(*pares, ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			listaPares = append(listaPares, p)
		}
	}

	b := broker.Novo(broker.Config{
		Endereco:       *endereco,
		Pares:          listaPares,
		TamanhoBuffer:  *buffer,
		MaxPendentes:   *maxPendentes,
		TimeoutEntrega: *timeoutEntrega,
		TimeoutPar:     *timeoutPar,
	})
	log.Fatal(b.Iniciar())
}
