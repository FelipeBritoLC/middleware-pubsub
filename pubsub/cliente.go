package pubsub

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	mrand "math/rand"
	"net"
	"sync"
	"time"

	"github.com/FelipeBritoLC/middleware-pubsub/protocolo"
)

var (
	ErrSemInscritos     = errors.New("mensagem descartada pelo broker: topico sem inscritos")
	ErrDesconectado     = errors.New("nao foi possivel conectar a nenhum broker")
	ErrFechado          = errors.New("cliente fechado")
	ErrTentativas       = errors.New("numero maximo de tentativas atingido")
	tipoConexaoPerdida  = "CONEXAO_PERDIDA"
	tamanhoHistorico    = 10000
	tamanhoFilaEntregas = 1000
)

type Mensagem struct {
	ID     string
	Topico string
	Dados  []byte
}

type Handler func(Mensagem)

type Cliente struct {
	Timeout    time.Duration
	Tentativas int

	brokers   []string
	proximo   int
	mu        sync.Mutex
	escritaMu sync.Mutex
	conn      net.Conn

	pendentes  map[string]chan protocolo.Mensagem
	inscricoes map[string]Handler

	vistos      map[string]bool
	ordemVistos []string

	entregas chan protocolo.Mensagem
	fechado  chan struct{}
	once     sync.Once
}

func Conectar(brokers []string) (*Cliente, error) {
	if len(brokers) == 0 {
		return nil, ErrDesconectado
	}
	c := &Cliente{
		Timeout:    15 * time.Second,
		Tentativas: 5,
		brokers:    brokers,
		proximo:    mrand.Intn(len(brokers)),
		pendentes:  make(map[string]chan protocolo.Mensagem),
		inscricoes: make(map[string]Handler),
		vistos:     make(map[string]bool),
		entregas:   make(chan protocolo.Mensagem, tamanhoFilaEntregas),
		fechado:    make(chan struct{}),
	}
	if err := c.conectar(); err != nil {
		return nil, err
	}
	go c.processarEntregas()
	return c, nil
}

func (c *Cliente) Publicar(topico string, dados []byte) (int, error) {
	m := protocolo.Mensagem{Tipo: protocolo.Publicar, ID: novoID(), Topico: topico, Dados: string(dados)}
	resp, err := c.requisitar(m)
	if err != nil {
		return 0, err
	}
	switch resp.Tipo {
	case protocolo.Ok:
		return resp.Entregues, nil
	case protocolo.SemInscritos:
		return 0, ErrSemInscritos
	default:
		return 0, errors.New(resp.Erro)
	}
}

func (c *Cliente) Inscrever(topico string, h Handler) error {
	c.mu.Lock()
	c.inscricoes[topico] = h
	c.mu.Unlock()
	_, err := c.requisitar(protocolo.Mensagem{Tipo: protocolo.Inscrever, ID: novoID(), Topico: topico})
	return err
}

func (c *Cliente) Desinscrever(topico string) error {
	c.mu.Lock()
	delete(c.inscricoes, topico)
	c.mu.Unlock()
	_, err := c.requisitar(protocolo.Mensagem{Tipo: protocolo.Desinscrever, ID: novoID(), Topico: topico})
	return err
}

func (c *Cliente) Fechar() {
	c.once.Do(func() {
		close(c.fechado)
		c.mu.Lock()
		if c.conn != nil {
			c.conn.Close()
		}
		c.mu.Unlock()
	})
}

func (c *Cliente) BrokerAtual() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		return ""
	}
	return c.conn.RemoteAddr().String()
}

func (c *Cliente) conectar() error {
	for i := 0; i < len(c.brokers); i++ {
		c.mu.Lock()
		endereco := c.brokers[c.proximo]
		c.proximo = (c.proximo + 1) % len(c.brokers)
		c.mu.Unlock()

		conn, err := net.DialTimeout("tcp", endereco, 2*time.Second)
		if err != nil {
			continue
		}

		c.mu.Lock()
		c.conn = conn
		topicos := make([]string, 0, len(c.inscricoes))
		for t := range c.inscricoes {
			topicos = append(topicos, t)
		}
		c.mu.Unlock()

		go c.ler(conn)

		for _, t := range topicos {
			c.enviar(protocolo.Mensagem{Tipo: protocolo.Inscrever, ID: novoID(), Topico: t})
		}
		return nil
	}
	return ErrDesconectado
}

func (c *Cliente) reconectar() {
	for {
		select {
		case <-c.fechado:
			return
		default:
		}
		if c.conectar() == nil {
			return
		}
		time.Sleep(time.Second)
	}
}

func (c *Cliente) ler(conn net.Conn) {
	dec := json.NewDecoder(conn)
	for {
		var m protocolo.Mensagem
		if err := dec.Decode(&m); err != nil {
			break
		}
		if m.Tipo == protocolo.Entrega {
			select {
			case c.entregas <- m:
			case <-c.fechado:
				return
			}
			continue
		}
		c.mu.Lock()
		ch := c.pendentes[m.ID]
		c.mu.Unlock()
		if ch != nil {
			select {
			case ch <- m:
			default:
			}
		}
	}

	conn.Close()
	c.mu.Lock()
	if c.conn == conn {
		c.conn = nil
	}
	for _, ch := range c.pendentes {
		select {
		case ch <- protocolo.Mensagem{Tipo: tipoConexaoPerdida}:
		default:
		}
	}
	c.mu.Unlock()

	select {
	case <-c.fechado:
	default:
		go c.reconectar()
	}
}

func (c *Cliente) enviar(m protocolo.Mensagem) error {
	c.mu.Lock()
	conn := c.conn
	c.mu.Unlock()
	if conn == nil {
		return ErrDesconectado
	}

	c.escritaMu.Lock()
	conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	err := json.NewEncoder(conn).Encode(m)
	c.escritaMu.Unlock()
	if err != nil {
		conn.Close()
	}
	return err
}

func (c *Cliente) esperarConexao() {
	limite := time.Now().Add(3 * time.Second)
	for time.Now().Before(limite) {
		c.mu.Lock()
		ok := c.conn != nil
		c.mu.Unlock()
		if ok {
			return
		}
		select {
		case <-c.fechado:
			return
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (c *Cliente) requisitar(m protocolo.Mensagem) (protocolo.Mensagem, error) {
	ch := make(chan protocolo.Mensagem, 1)
	c.mu.Lock()
	c.pendentes[m.ID] = ch
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(c.pendentes, m.ID)
		c.mu.Unlock()
	}()

	ultimoErro := ErrTentativas
	for t := 0; t < c.Tentativas; t++ {
		select {
		case <-c.fechado:
			return protocolo.Mensagem{}, ErrFechado
		default:
		}

		if err := c.enviar(m); err != nil {
			ultimoErro = err
			c.esperarConexao()
			continue
		}

		select {
		case resp := <-ch:
			if resp.Tipo == tipoConexaoPerdida {
				ultimoErro = ErrDesconectado
				c.esperarConexao()
				continue
			}
			if resp.Tipo == protocolo.Erro {
				ultimoErro = errors.New(resp.Erro)
				time.Sleep(500 * time.Millisecond)
				continue
			}
			return resp, nil
		case <-time.After(c.Timeout):
			ultimoErro = ErrTentativas
		case <-c.fechado:
			return protocolo.Mensagem{}, ErrFechado
		}
	}
	return protocolo.Mensagem{}, ultimoErro
}

func (c *Cliente) processarEntregas() {
	for {
		select {
		case m := <-c.entregas:
			if !c.jaRecebida(m.ID) {
				c.mu.Lock()
				h := c.inscricoes[m.Topico]
				c.mu.Unlock()
				if h != nil {
					h(Mensagem{ID: m.ID, Topico: m.Topico, Dados: []byte(m.Dados)})
				}
			}
			c.enviar(protocolo.Mensagem{Tipo: protocolo.Confirmacao, ID: m.ID})
		case <-c.fechado:
			return
		}
	}
}

func (c *Cliente) jaRecebida(id string) bool {
	if c.vistos[id] {
		return true
	}
	c.vistos[id] = true
	c.ordemVistos = append(c.ordemVistos, id)
	if len(c.ordemVistos) > tamanhoHistorico {
		delete(c.vistos, c.ordemVistos[0])
		c.ordemVistos = c.ordemVistos[1:]
	}
	return false
}

func novoID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return hex.EncodeToString(b)
}
