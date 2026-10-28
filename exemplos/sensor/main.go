package main

import (
	"flag"
	"log"
	"time"

	"github.com/FelipeBritoLC/middleware-pubsub/exemplos/leitura"
	"github.com/FelipeBritoLC/middleware-pubsub/pubsub"
)

func main() {
	brokers := flag.String("brokers", "localhost:7001,localhost:7002,localhost:7003", "lista de brokers")
	id := flag.String("id", "sensor-1", "identificador do sensor")
	topico := flag.String("topico", "temperatura/costa-norte", "topico onde as leituras sao publicadas")
	intervalo := flag.Duration("intervalo", 2*time.Second, "intervalo entre leituras")
	online := flag.Duration("online", 0, "tempo conectado antes de ficar offline (0 = sempre online)")
	offline := flag.Duration("offline", 20*time.Second, "tempo offline quando -online for usado")
	flag.Parse()

	lista := leitura.ListaBrokers(*brokers)
	var buffer []leitura.Leitura
	var cliente *pubsub.Cliente
	seq := 0
	inicioCiclo := time.Now()
	conectado := true

	for {
		seq++
		buffer = append(buffer, leitura.Nova(*id, seq))

		if *online > 0 {
			if conectado && time.Since(inicioCiclo) > *online {
				conectado = false
				inicioCiclo = time.Now()
				if cliente != nil {
					cliente.Fechar()
					cliente = nil
				}
				log.Printf("[%s] ficando offline por %v", *id, *offline)
			} else if !conectado && time.Since(inicioCiclo) > *offline {
				conectado = true
				inicioCiclo = time.Now()
				log.Printf("[%s] voltando a ficar online com %d leituras no buffer", *id, len(buffer))
			}
		}

		if conectado {
			if cliente == nil {
				c, err := pubsub.Conectar(lista)
				if err != nil {
					log.Printf("[%s] sem broker disponivel, leitura fica no buffer (%d)", *id, len(buffer))
				} else {
					cliente = c
					log.Printf("[%s] conectado em %s", *id, cliente.BrokerAtual())
				}
			}
			if cliente != nil {
				buffer = enviarBuffer(cliente, *id, *topico, buffer)
			}
		}

		time.Sleep(*intervalo)
	}
}

func enviarBuffer(c *pubsub.Cliente, id, topico string, buffer []leitura.Leitura) []leitura.Leitura {
	for len(buffer) > 0 {
		l := buffer[0]
		n, err := c.Publicar(topico, l.Bytes())
		if err == pubsub.ErrSemInscritos {
			log.Printf("[%s] leitura %d descartada pelo broker (sem inscritos), mantida no buffer local", id, l.Seq)
			return buffer
		}
		if err != nil {
			log.Printf("[%s] erro ao publicar leitura %d: %v", id, l.Seq, err)
			return buffer
		}
		log.Printf("[%s] leitura %d (%.2f C) entregue a %d inscrito(s)", id, l.Seq, l.Temperatura, n)
		buffer = buffer[1:]
	}
	return buffer
}
