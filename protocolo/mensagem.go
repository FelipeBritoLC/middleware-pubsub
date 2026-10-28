package protocolo

const (
	Publicar     = "PUB"
	Inscrever    = "SUB"
	Desinscrever = "UNSUB"
	Encaminhar   = "FWD"
	Entrega      = "MSG"
	Confirmacao  = "ACK"
	Ok           = "OK"
	SemInscritos = "SEM_INSCRITOS"
	Erro         = "ERRO"
)

type Mensagem struct {
	Tipo      string `json:"tipo"`
	ID        string `json:"id,omitempty"`
	Topico    string `json:"topico,omitempty"`
	Dados     string `json:"dados,omitempty"`
	Entregues int    `json:"entregues,omitempty"`
	Erro      string `json:"erro,omitempty"`
}
