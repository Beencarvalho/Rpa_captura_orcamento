# API Budget Data Pipeline

Este projeto tem como objetivo coletar dados de orçamentos de uma API, armazená-los em um banco de dados temporário (DuckDB) e gerar relatórios em formato Excel. Ele automatiza o processo de requisição e armazenamento de dados, oferecendo uma solução eficiente para a análise de informações financeiras.

## Funcionalidades

- Requisição de dados de orçamentos a partir de uma API.
- Tratamento de respostas e tratamento de erros durante a conexão.
- Armazenamento dos dados em um banco de dados temporário utilizando DuckDB.
- Geração de relatórios em Excel a partir dos dados coletados e processados.

## Pré-requisitos

- Python 3.7+
- Pip
- DuckDB
- Pandas
- Requests

## Instalação

Clone este repositório para sua máquina local e instale as dependências:

```bash
1. Clone esse repositório
2. Definir a versao do Python usando o `pyenv local 3.12.1`
`poetry env use 3.12.1`, 
`poetry install --no-root`
`poetry lock --no-update`
```

## Configuração

Antes de executar o script, garanta que você tenha os tokens de API configurados no arquivo `util/api_token.py`. Solicite ao equipe de TI o arquivo api_token.py, e o coloque na pasta "app/util" do projeto. 

## Uso

Para executar o script, siga os seguintes passos:

```bash
python main.py
```

### Parâmetros

- **api_budget**: URL para obtenção de todos os orçamentos.
- **api_budget_months**: URL para obter os detalhes mensais de cada orçamento.

### Saída

- `Validacao dos Dados SGO.xlsx`: Contém informações gerais dos orçamentos.
- `Controladoria.xlsx`: Relatório detalhado para controladoria.

## Estrutura do Projeto

- **OrcamentoSGO.py**: Script principal que executa todo o pipeline de dados.
- **util/api_token.py**: Contém os tokens de API necessários para autenticação.

## Erros Comuns e Soluções

- **Erro de autenticação (401)**: Verifique o token de acesso no arquivo `api_token.py`.
- **Recurso não encontrado (404)**: Verifique as URLs configuradas para a API.
- **Erro interno do servidor (500)**: Tente novamente mais tarde, pois o problema é do lado do servidor.

## Contribuição

Contribuições são bem-vindas! Sinta-se à vontade para abrir uma _issue_ ou enviar um _pull request_ para melhorias.

## Licença

Este projeto está sob a licença MIT e  sobre uso da SANTA CASA DA BAHIA
