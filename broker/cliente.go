package broker

import (
	"encoding/json"
	"net"
	"sync"
	"time"

	"github.com/FelipeBritoLC/middleware-pubsub/protocolo"
)

type cliente struct {
	conn      net.Conn
	saida     chan protocolo.Mensagem
	fechado   chan struct{}
	once      sync.Once
	mu        sync.Mutex
	pendentes map[string]chan struct{}
}

func novoCliente(conn net.Conn, tamanhoBuffer int) *cliente {
	return &cliente{
		conn:      conn,
		saida:     make(chan protocolo.Mensagem, tamanhoBuffer),
		fechado:   make(chan struct{}),
		pendentes: make(map[string]chan struct{}),
	}
}

func (c *cliente) escrever() {
	enc := json.NewEncoder(c.conn)
	for {
		select {
		case m := <-c.saida:
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := enc.Encode(m); err != nil {
				c.fechar()
				return
			}
		case <-c.fechado:
			return
		}
	}
}

func (c *cliente) enviar(m protocolo.Mensagem, timeout time.Duration) bool {
	select {
	case c.saida <- m:
		return true
	case <-c.fechado:
		return false
	case <-time.After(timeout):
		return false
	}
}

func (c *cliente) entregar(m protocolo.Mensagem, timeout time.Duration) bool {
	c.mu.Lock()
	ch, ok := c.pendentes[m.ID]
	if !ok {
		ch = make(chan struct{})
		c.pendentes[m.ID] = ch
	}
	c.mu.Unlock()

	entrega := protocolo.Mensagem{Tipo: protocolo.Entrega, ID: m.ID, Topico: m.Topico, Dados: m.Dados}
	if !c.enviar(entrega, timeout) {
		c.removerPendente(m.ID, ch)
		return false
	}

	select {
	case <-ch:
		return true
	case <-c.fechado:
		return false
	case <-time.After(timeout):
		c.removerPendente(m.ID, ch)
		return false
	}
}

func (c *cliente) removerPendente(id string, ch chan struct{}) {
	c.mu.Lock()
	if c.pendentes[id] == ch {
		delete(c.pendentes, id)
	}
	c.mu.Unlock()
}

func (c *cliente) confirmar(id string) {
	c.mu.Lock()
	if ch, ok := c.pendentes[id]; ok {
		close(ch)
		delete(c.pendentes, id)
	}
	c.mu.Unlock()
}

func (c *cliente) fechar() {
	c.once.Do(func() {
		close(c.fechado)
		c.conn.Close()
	})
}
