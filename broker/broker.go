package broker

import (
	"bufio"
	"encoding/json"
	"errors"
	"log"
	"net"
	"sync"
	"time"

	"github.com/FelipeBritoLC/middleware-pubsub/protocolo"
)

type Config struct {
	Endereco       string
	Pares          []string
	TamanhoBuffer  int
	MaxPendentes   int
	TimeoutEntrega time.Duration
	TimeoutPar     time.Duration
}

type Broker struct {
	cfg     Config
	mu      sync.Mutex
	topicos map[string]map[*cliente]bool
}

var errParIndisponivel = errors.New("par indisponivel")

func Novo(cfg Config) *Broker {
	return &Broker{
		cfg:     cfg,
		topicos: make(map[string]map[*cliente]bool),
	}
}

func (b *Broker) Iniciar() error {
	ln, err := net.Listen("tcp", b.cfg.Endereco)
	if err != nil {
		return err
	}
	log.Printf("broker escutando em %s (pares: %v)", b.cfg.Endereco, b.cfg.Pares)
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("erro no accept: %v", err)
			continue
		}
		go b.atender(conn)
	}
}

func (b *Broker) atender(conn net.Conn) {
	c := novoCliente(conn, b.cfg.TamanhoBuffer)
	go c.escrever()
	defer b.removerCliente(c)

	dec := json.NewDecoder(bufio.NewReader(conn))
	limite := make(chan struct{}, b.cfg.MaxPendentes)

	for {
		var m protocolo.Mensagem
		if err := dec.Decode(&m); err != nil {
			return
		}

		switch m.Tipo {
		case protocolo.Publicar, protocolo.Encaminhar:
			limite <- struct{}{}
			go func(m protocolo.Mensagem) {
				defer func() { <-limite }()
				resp := b.publicar(m, m.Tipo == protocolo.Publicar)
				c.enviar(resp, b.cfg.TimeoutEntrega)
			}(m)
		case protocolo.Inscrever:
			b.inscrever(c, m.Topico)
			c.enviar(protocolo.Mensagem{Tipo: protocolo.Ok, ID: m.ID, Topico: m.Topico}, b.cfg.TimeoutEntrega)
		case protocolo.Desinscrever:
			b.desinscrever(c, m.Topico)
			c.enviar(protocolo.Mensagem{Tipo: protocolo.Ok, ID: m.ID, Topico: m.Topico}, b.cfg.TimeoutEntrega)
		case protocolo.Confirmacao:
			c.confirmar(m.ID)
		default:
			c.enviar(protocolo.Mensagem{Tipo: protocolo.Erro, ID: m.ID, Erro: "tipo de mensagem desconhecido"}, b.cfg.TimeoutEntrega)
		}
	}
}

func (b *Broker) inscrever(c *cliente, topico string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.topicos[topico] == nil {
		b.topicos[topico] = make(map[*cliente]bool)
		log.Printf("topico criado: %s", topico)
	}
	b.topicos[topico][c] = true
}

func (b *Broker) desinscrever(c *cliente, topico string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.removerInscricao(c, topico)
}

func (b *Broker) removerInscricao(c *cliente, topico string) {
	inscritos, ok := b.topicos[topico]
	if !ok {
		return
	}
	delete(inscritos, c)
	if len(inscritos) == 0 {
		delete(b.topicos, topico)
		log.Printf("topico removido: %s", topico)
	}
}

func (b *Broker) removerCliente(c *cliente) {
	c.fechar()
	b.mu.Lock()
	defer b.mu.Unlock()
	for topico, inscritos := range b.topicos {
		if inscritos[c] {
			b.removerInscricao(c, topico)
		}
	}
}

func (b *Broker) inscritos(topico string) []*cliente {
	b.mu.Lock()
	defer b.mu.Unlock()
	lista := make([]*cliente, 0, len(b.topicos[topico]))
	for c := range b.topicos[topico] {
		lista = append(lista, c)
	}
	return lista
}

func (b *Broker) publicar(m protocolo.Mensagem, encaminhar bool) protocolo.Mensagem {
	var wg sync.WaitGroup
	var mu sync.Mutex
	total := 0
	falhou := false

	for _, c := range b.inscritos(m.Topico) {
		wg.Add(1)
		go func(c *cliente) {
			defer wg.Done()
			ok := c.entregar(m, b.cfg.TimeoutEntrega)
			mu.Lock()
			if ok {
				total++
			} else {
				falhou = true
			}
			mu.Unlock()
		}(c)
	}

	if encaminhar {
		for _, par := range b.cfg.Pares {
			wg.Add(1)
			go func(par string) {
				defer wg.Done()
				n, err := b.encaminhar(par, m)
				if err == errParIndisponivel {
					return
				}
				mu.Lock()
				if err != nil {
					falhou = true
				} else {
					total += n
				}
				mu.Unlock()
			}(par)
		}
	}

	wg.Wait()

	resp := protocolo.Mensagem{ID: m.ID, Topico: m.Topico}
	switch {
	case falhou:
		resp.Tipo = protocolo.Erro
		resp.Erro = "falha ao entregar para algum inscrito"
	case total == 0:
		resp.Tipo = protocolo.SemInscritos
		if encaminhar {
			log.Printf("mensagem %s descartada: topico %s sem inscritos", m.ID, m.Topico)
		}
	default:
		resp.Tipo = protocolo.Ok
		resp.Entregues = total
	}
	return resp
}

func (b *Broker) encaminhar(par string, m protocolo.Mensagem) (int, error) {
	conn, err := net.DialTimeout("tcp", par, time.Second)
	if err != nil {
		return 0, errParIndisponivel
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(b.cfg.TimeoutPar))

	m.Tipo = protocolo.Encaminhar
	if err := json.NewEncoder(conn).Encode(m); err != nil {
		return 0, err
	}

	var resp protocolo.Mensagem
	if err := json.NewDecoder(conn).Decode(&resp); err != nil {
		return 0, err
	}

	switch resp.Tipo {
	case protocolo.Ok:
		return resp.Entregues, nil
	case protocolo.SemInscritos:
		return 0, nil
	default:
		return 0, errors.New(resp.Erro)
	}
}
