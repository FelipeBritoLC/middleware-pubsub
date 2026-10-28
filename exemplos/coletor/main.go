package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"

	"github.com/FelipeBritoLC/middleware-pubsub/exemplos/leitura"
	"github.com/FelipeBritoLC/middleware-pubsub/pubsub"
)

func main() {
	brokers := flag.String("brokers", "localhost:7001,localhost:7002,localhost:7003", "lista de brokers")
	topicos := flag.String("topicos", "temperatura/costa-norte,temperatura/costa-sul", "topicos para se inscrever")
	flag.Parse()

	c, err := pubsub.Conectar(leitura.ListaBrokers(*brokers))
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("coletor conectado em %s", c.BrokerAtual())

	inscritos := map[string]bool{}
	for _, t := range leitura.ListaBrokers(*topicos) {
		inscrever(c, t)
		inscritos[t] = true
	}

	fmt.Println("comandos: 'inscrever <topico>', 'desinscrever <topico>', 'sair'")

	sinal := make(chan os.Signal, 1)
	signal.Notify(sinal, os.Interrupt)
	comandos := make(chan string)
	go func() {
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			comandos <- scanner.Text()
		}
	}()

	for {
		select {
		case <-sinal:
			sair(c, inscritos)
			return
		case linha := <-comandos:
			partes := strings.Fields(linha)
			if len(partes) == 0 {
				continue
			}
			switch {
			case partes[0] == "sair":
				sair(c, inscritos)
				return
			case partes[0] == "inscrever" && len(partes) == 2:
				inscrever(c, partes[1])
				inscritos[partes[1]] = true
			case partes[0] == "desinscrever" && len(partes) == 2:
				if err := c.Desinscrever(partes[1]); err != nil {
					log.Printf("erro ao desinscrever: %v", err)
				} else {
					delete(inscritos, partes[1])
					log.Printf("inscricao removida: %s", partes[1])
				}
			default:
				fmt.Println("comando invalido")
			}
		}
	}
}

func inscrever(c *pubsub.Cliente, topico string) {
	err := c.Inscrever(topico, func(m pubsub.Mensagem) {
		l, err := leitura.Ler(m.Dados)
		if err != nil {
			log.Printf("mensagem invalida em %s: %s", m.Topico, string(m.Dados))
			return
		}
		log.Printf("[%s] %s seq=%d temperatura=%.2f C latencia=%v", m.Topico, l.Sensor, l.Seq, l.Temperatura, leitura.Latencia(l))
	})
	if err != nil {
		log.Printf("erro ao se inscrever em %s: %v", topico, err)
		return
	}
	log.Printf("inscrito em %s", topico)
}

func sair(c *pubsub.Cliente, inscritos map[string]bool) {
	for t := range inscritos {
		c.Desinscrever(t)
	}
	c.Fechar()
	log.Println("coletor encerrado")
}
