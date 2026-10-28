package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/FelipeBritoLC/middleware-pubsub/exemplos/leitura"
	"github.com/FelipeBritoLC/middleware-pubsub/pubsub"
)

type registro struct {
	coletor  int
	sensor   string
	seq      int
	latencia time.Duration
	recebida time.Duration
}

type publicacao struct {
	sensor  string
	inicio  time.Duration
	duracao time.Duration
}

type teste struct {
	brokers     []string
	inicio      time.Time
	fimGeracao  time.Time
	prazoFinal  time.Time
	mu          sync.Mutex
	geradas     map[string]int
	perfis      map[string]string
	recebidas   []map[string]bool
	registros   []registro
	duplicadas  int64
	ok          int64
	semInscrito int64
	erros       int64
	publicacoes []publicacao
	trocas      []string
}

func main() {
	brokers := flag.String("brokers", "localhost:7001,localhost:7002,localhost:7003", "lista de brokers")
	duracao := flag.Duration("duracao", 60*time.Second, "tempo gerando leituras")
	espera := flag.Duration("espera", 60*time.Second, "tempo maximo para esvaziar os buffers dos sensores depois do fim")
	continuos := flag.Int("continuos", 20, "sensores com fluxo continuo (1 leitura a cada 500ms)")
	raros := flag.Int("raros", 30, "sensores que enviam poucas leituras (1 a cada 5-15s)")
	intermitentes := flag.Int("intermitentes", 20, "sensores que ficam offline periodicamente")
	rajadas := flag.Int("rajadas", 2, "sensores que enviam rajadas de 100 leituras")
	travados := flag.Int("travados", 5, "conexoes que travam no meio de uma mensagem (ma conexao)")
	coletores := flag.Int("coletores", 2, "quantidade de coletores inscritos")
	regioes := flag.Int("regioes", 4, "quantidade de topicos (regioes)")
	saida := flag.String("saida", "resultados", "pasta onde os resultados sao salvos")
	flag.Parse()

	t := &teste{
		brokers: leitura.ListaBrokers(*brokers),
		inicio:  time.Now(),
		geradas: map[string]int{},
		perfis:  map[string]string{},
	}

	var topicos []string
	for i := 1; i <= *regioes; i++ {
		topicos = append(topicos, fmt.Sprintf("temperatura/regiao-%d", i))
	}

	var clientesColetores []*pubsub.Cliente
	for i := 0; i < *coletores; i++ {
		c, err := t.iniciarColetor(i, topicos)
		if err != nil {
			log.Fatalf("erro ao iniciar coletor %d: %v", i, err)
		}
		clientesColetores = append(clientesColetores, c)
	}

	parar := make(chan struct{})
	for i, c := range clientesColetores {
		go t.vigiarColetor(i, c, parar)
	}
	for i := 0; i < *travados; i++ {
		go conexaoTravada(t.brokers[i%len(t.brokers)], parar)
	}

	inicio := time.Now()
	t.fimGeracao = inicio.Add(*duracao)
	t.prazoFinal = t.fimGeracao.Add(*espera)

	var wg sync.WaitGroup
	criar := func(qtd int, perfil string, f func(id, topico string)) {
		for i := 0; i < qtd; i++ {
			id := fmt.Sprintf("%s-%d", perfil, i+1)
			topico := topicos[rand.Intn(len(topicos))]
			t.mu.Lock()
			t.perfis[id] = perfil
			t.mu.Unlock()
			wg.Add(1)
			go func() {
				defer wg.Done()
				f(id, topico)
			}()
		}
	}

	criar(*continuos, "continuo", func(id, topico string) {
		t.sensor(id, topico, func() time.Duration { return 500 * time.Millisecond }, false)
	})
	criar(*raros, "raro", func(id, topico string) {
		t.sensor(id, topico, func() time.Duration { return time.Duration(5+rand.Intn(11)) * time.Second }, false)
	})
	criar(*intermitentes, "intermitente", func(id, topico string) {
		t.sensor(id, topico, func() time.Duration { return time.Second }, true)
	})
	criar(*rajadas, "rajada", func(id, topico string) {
		t.sensorRajada(id, topico)
	})

	log.Printf("teste iniciado: %d sensores, %d coletores, %d conexoes travadas, duracao %v",
		*continuos+*raros+*intermitentes+*rajadas, *coletores, *travados, *duracao)

	wg.Wait()
	time.Sleep(2 * time.Second)
	close(parar)
	for _, c := range clientesColetores {
		c.Fechar()
	}

	t.relatorio(*saida, time.Since(inicio))
}

func (t *teste) iniciarColetor(n int, topicos []string) (*pubsub.Cliente, error) {
	c, err := pubsub.Conectar(t.brokers)
	if err != nil {
		return nil, err
	}
	t.mu.Lock()
	t.recebidas = append(t.recebidas, map[string]bool{})
	t.mu.Unlock()

	for _, topico := range topicos {
		err := c.Inscrever(topico, func(m pubsub.Mensagem) {
			l, err := leitura.Ler(m.Dados)
			if err != nil {
				return
			}
			chave := fmt.Sprintf("%s#%d", l.Sensor, l.Seq)
			t.mu.Lock()
			defer t.mu.Unlock()
			if t.recebidas[n][chave] {
				t.duplicadas++
				return
			}
			t.recebidas[n][chave] = true
			t.registros = append(t.registros, registro{coletor: n, sensor: l.Sensor, seq: l.Seq, latencia: leitura.Latencia(l), recebida: time.Since(t.inicio)})
		})
		if err != nil {
			return nil, err
		}
	}
	log.Printf("coletor %d inscrito em %v via %s", n, topicos, c.BrokerAtual())
	return c, nil
}

func (t *teste) gerar(id string) leitura.Leitura {
	t.mu.Lock()
	t.geradas[id]++
	seq := t.geradas[id]
	t.mu.Unlock()
	return leitura.Nova(id, seq)
}

func (t *teste) publicar(c *pubsub.Cliente, topico string, l leitura.Leitura) bool {
	comeco := time.Now()
	_, err := c.Publicar(topico, l.Bytes())
	t.mu.Lock()
	t.publicacoes = append(t.publicacoes, publicacao{sensor: l.Sensor, inicio: comeco.Sub(t.inicio), duracao: time.Since(comeco)})
	t.mu.Unlock()
	switch err {
	case nil:
		atomic.AddInt64(&t.ok, 1)
		return true
	case pubsub.ErrSemInscritos:
		atomic.AddInt64(&t.semInscrito, 1)
	default:
		atomic.AddInt64(&t.erros, 1)
	}
	return false
}

func (t *teste) sensor(id, topico string, intervalo func() time.Duration, intermitente bool) {
	var buffer []leitura.Leitura
	var c *pubsub.Cliente
	conectado := true
	trocaEstado := time.Now().Add(time.Duration(5+rand.Intn(6)) * time.Second)
	proximaLeitura := time.Now()

	defer func() {
		if c != nil {
			c.Fechar()
		}
	}()

	for {
		agora := time.Now()
		gerando := agora.Before(t.fimGeracao)

		if gerando && !agora.Before(proximaLeitura) {
			buffer = append(buffer, t.gerar(id))
			proximaLeitura = agora.Add(intervalo())
		}

		if intermitente {
			if !gerando {
				conectado = true
			} else if agora.After(trocaEstado) {
				conectado = !conectado
				if conectado {
					trocaEstado = agora.Add(time.Duration(5+rand.Intn(6)) * time.Second)
				} else {
					trocaEstado = agora.Add(time.Duration(10+rand.Intn(11)) * time.Second)
					if c != nil {
						c.Fechar()
						c = nil
					}
				}
			}
		}

		if conectado {
			if c == nil {
				novo, err := pubsub.Conectar(t.brokers)
				if err == nil {
					c = novo
				}
			}
			for c != nil && len(buffer) > 0 {
				if !t.publicar(c, topico, buffer[0]) {
					break
				}
				buffer = buffer[1:]
			}
		}

		if !gerando && len(buffer) == 0 {
			return
		}
		if agora.After(t.prazoFinal) {
			log.Printf("[%s] prazo esgotado com %d leituras no buffer", id, len(buffer))
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (t *teste) sensorRajada(id, topico string) {
	var c *pubsub.Cliente
	for c == nil {
		novo, err := pubsub.Conectar(t.brokers)
		if err != nil {
			time.Sleep(time.Second)
			continue
		}
		c = novo
	}
	defer c.Fechar()

	time.Sleep(time.Duration(rand.Intn(5)) * time.Second)
	for time.Now().Before(t.fimGeracao) {
		var wg sync.WaitGroup
		for i := 0; i < 100; i++ {
			l := t.gerar(id)
			wg.Add(1)
			go func() {
				defer wg.Done()
				for !t.publicar(c, topico, l) {
					if time.Now().After(t.prazoFinal) {
						return
					}
					time.Sleep(500 * time.Millisecond)
				}
			}()
		}
		wg.Wait()
		time.Sleep(15 * time.Second)
	}
}

func (t *teste) vigiarColetor(n int, c *pubsub.Cliente, parar chan struct{}) {
	atual := c.BrokerAtual()
	for {
		select {
		case <-parar:
			return
		case <-time.After(50 * time.Millisecond):
		}
		novo := c.BrokerAtual()
		if novo != atual {
			msg := fmt.Sprintf("%.2fs coletor %d: '%s' -> '%s'", time.Since(t.inicio).Seconds(), n, atual, novo)
			log.Print(msg)
			t.mu.Lock()
			t.trocas = append(t.trocas, msg)
			t.mu.Unlock()
			atual = novo
		}
	}
}

func conexaoTravada(broker string, parar chan struct{}) {
	conn, err := net.Dial("tcp", broker)
	if err != nil {
		return
	}
	defer conn.Close()
	conn.Write([]byte(`{"tipo":"PUB","id":"travado","topico":"temperatura/regiao-1","dados":"`))
	<-parar
}

func percentil(valores []float64, p float64) float64 {
	if len(valores) == 0 {
		return 0
	}
	i := int(float64(len(valores)-1) * p)
	return valores[i]
}

func (t *teste) relatorio(pasta string, duracao time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()

	totalGeradas := 0
	for _, n := range t.geradas {
		totalGeradas += n
	}

	unicas := map[string]bool{}
	for _, r := range t.recebidas {
		for k := range r {
			unicas[k] = true
		}
	}

	var sb strings.Builder
	fmt.Fprintf(&sb, "===== RESULTADO DO TESTE =====\n")
	fmt.Fprintf(&sb, "duracao total: %v\n", duracao.Round(time.Second))
	fmt.Fprintf(&sb, "leituras geradas pelos sensores: %d\n", totalGeradas)
	fmt.Fprintf(&sb, "publicacoes confirmadas (OK): %d\n", t.ok)
	fmt.Fprintf(&sb, "respostas SEM_INSCRITOS: %d\n", t.semInscrito)
	fmt.Fprintf(&sb, "publicacoes com erro (reenviadas depois): %d\n", t.erros)
	fmt.Fprintf(&sb, "leituras recebidas por pelo menos um coletor: %d\n", len(unicas))
	perdidas := totalGeradas - len(unicas)
	fmt.Fprintf(&sb, "leituras perdidas: %d (%.3f%%)\n", perdidas, 100*float64(perdidas)/float64(max(totalGeradas, 1)))
	fmt.Fprintf(&sb, "duplicatas entregues a aplicacao: %d\n", t.duplicadas)
	for i, r := range t.recebidas {
		fmt.Fprintf(&sb, "coletor %d recebeu %d leituras (%.2f%%)\n", i, len(r), 100*float64(len(r))/float64(max(totalGeradas, 1)))
	}

	porPerfil := map[string][]float64{}
	for _, r := range t.registros {
		ms := float64(r.latencia.Microseconds()) / 1000
		porPerfil[t.perfis[r.sensor]] = append(porPerfil[t.perfis[r.sensor]], ms)
		porPerfil["todos"] = append(porPerfil["todos"], ms)
	}

	fmt.Fprintf(&sb, "\nlatencia (geracao da leitura -> chegada no coletor), em ms:\n")
	fmt.Fprintf(&sb, "%-14s %8s %10s %10s %10s %10s %10s\n", "perfil", "qtd", "media", "p50", "p95", "p99", "max")
	perfis := []string{"continuo", "raro", "intermitente", "rajada", "todos"}
	for _, p := range perfis {
		v := porPerfil[p]
		if len(v) == 0 {
			continue
		}
		sort.Float64s(v)
		soma := 0.0
		for _, x := range v {
			soma += x
		}
		fmt.Fprintf(&sb, "%-14s %8d %10.2f %10.2f %10.2f %10.2f %10.2f\n", p, len(v), soma/float64(len(v)),
			percentil(v, 0.5), percentil(v, 0.95), percentil(v, 0.99), v[len(v)-1])
	}

	if len(t.trocas) > 0 {
		fmt.Fprintf(&sb, "\ntrocas de broker dos coletores:\n")
		for _, tr := range t.trocas {
			fmt.Fprintf(&sb, "%s\n", tr)
		}
	}

	fmt.Print(sb.String())

	os.MkdirAll(pasta, 0755)
	os.WriteFile(filepath.Join(pasta, "resumo.txt"), []byte(sb.String()), 0644)

	f, err := os.Create(filepath.Join(pasta, "entregas.csv"))
	if err != nil {
		log.Printf("erro ao salvar csv: %v", err)
		return
	}
	defer f.Close()
	fmt.Fprintln(f, "coletor,sensor,perfil,seq,latencia_ms,recebida_s")
	for _, r := range t.registros {
		fmt.Fprintf(f, "%d,%s,%s,%d,%.3f,%.3f\n", r.coletor, r.sensor, t.perfis[r.sensor], r.seq, float64(r.latencia.Microseconds())/1000, r.recebida.Seconds())
	}
	p, err := os.Create(filepath.Join(pasta, "publicacoes.csv"))
	if err == nil {
		fmt.Fprintln(p, "sensor,perfil,inicio_s,duracao_ms")
		for _, pb := range t.publicacoes {
			fmt.Fprintf(p, "%s,%s,%.3f,%.3f\n", pb.sensor, t.perfis[pb.sensor], pb.inicio.Seconds(), float64(pb.duracao.Microseconds())/1000)
		}
		p.Close()
	}
	fmt.Printf("\nresultados salvos em %s/\n", pasta)
}
