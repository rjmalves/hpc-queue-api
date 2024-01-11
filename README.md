# hpc-queue-api

API de gerenciamento de filas para clusters HPC, de forma assíncrona. Esta API é hospedada em um nó do cluster (geralmente o nó mestre) e interage com programas de gerenciamento de filas específicos, como o [PBS Torque](https://hpc-wiki.info/hpc/Torque), o [OGS/GE](https://gridscheduler.sourceforge.net/) e o [Slurm](https://slurm.schedmd.com/squeue.html). Para o usuário, é fornecida uma REST API que abstrai o programa de gerenciamento de filas que está sendo executado, e o usuário pode submeter, ler e deletar jobs através de requisições HTTP.

A API assume a existência de alguns scripts shell no cluster HPC, que executam as tarefas desejadas pelo usuário, realizando apenas a tradução para os comandos específicos de cada gerenciador de filas ao realizar a reserva de recursos adequada, identificação dos jobs e apontamento para o diretório de trabalho onde os scripts shell de cada job devem ser executados. 

A concepção desta API foi para realizar submissão de jobs para os modelos de planejamento energético (NEWAVE, DECOMP, DESSEM) que são executados em ambiente HPC e, portanto, existem regras de negócio específicas para encontrar os scripts dos modelos em diretórios de rede pré-estabelecidos.

Atualmente, existe a possibilidade de se executar a API com uma dependência adiciona do [tuber](https://github.com/marianasnoel/tuber), que generaliza algumas opções de execução e torna os jobs dos modelos mais flexíveis ao delegar parte do processamento para rotinas em `Python`.

## Instalação

Para realizar a instalação a partir do repositório, é recomendado criar um ambiente virtual e realizar a instalação das dependências dentro do mesmo.

```
$ git clone https://github.com/rjmalves/hpc-queue-api
$ cd hpc-queue-api
$ python3 -m venv ./venv
$ source ./venv/bin/activate
$ pip install -r requirements.txt
```

## Configuração

A configuração do monitor pode ser feita através de um arquivo de variáveis de ambiente `.env`, existente no próprio diretório de instalação. O conteúdo deste arquivo:

```
CLUSTER_ID=1
SCHEDULER="SGE"
PROGRAM_PATH_RULE="TUBER"
HOST="0.0.0.0"
PORT=5049
ROOT_PATH="/api/v1/queue"
```

Cada deploy da `hpc-queue-api` deve ter um atributo `CLUSTER_ID` único, para que outros serviços possam controlar atividades em clusters distintos. O gerenciador de filas existente no cluster é especificado em `SCHEDULER` e atualmente são suportados `SGE` ou `TORQUE`.

A configuração `PROGRAM_PATH_RULE` contém qual conjunto de regras de negócio que a API deve considerar para realizar a localização dos shell scripts que executam os modelos de planejamento energético. Atualmente são suportadas `PEMAWS` (organização em diretório legada utilizada pela PEM) e `TUBER`, quando utilizado um deploy em conjunto com o repositório mencionado anteriormente.

Atualmente as opções suportadas são:

|       Campo       |   Valores aceitos   |
| ----------------- | ------------------- |
| CLUSTER_ID        | `int`               |
| SCHEDULER         | `str`               |
| PROGRAM_PATH_RULE | `str`               |
| HOST              | `str`               |
| PORT              | `int`               |
| ROOT_PATH         | `str` (URL prefix)  |


## Uso

Para executar o programa, basta interpretar o arquivo `main.py`:

```
$ source ./venv/bin/activate
$ python main.py
```

No terminal é impresso um log de acompanhamento:

```
INFO:     Started server process [2133]
INFO:     Waiting for application startup.
INFO:     Application startup complete.
INFO:     Uvicorn running on http://0.0.0.0:5043 (Press CTRL+C to quit)
INFO:     127.0.0.1:36872 - "GET /docs HTTP/1.1" 200 OK
INFO:     127.0.0.1:36872 - "GET /openapi.json HTTP/1.1" 200 OK
```

Maiores detalhes sobre as rotas disponíveis na API pode ser visto ao lançar a aplicação localmente e acessar a rota `/docs`, que possui uma página no formato [OpenAPI](https://swagger.io/specification/). Em geral, toda a interface é feita com base nos objetos fundamentais `Job` e `Program`, que possuem as especificações:


1. Job

```json
[
  {
    "jobId": "string",
    "status": "START_REQUESTED",
    "name": "string",
    "startTime": "2024-01-08T17:25:16.629Z",
    "lastStatusUpdateTime": "2024-01-08T17:25:16.629Z",
    "endTime": "2024-01-08T17:25:16.629Z",
    "clusterId": "string",
    "workingDirectory": "string",
    "reservedSlots": 0,
    "scriptFile": "string",
    "args": [
      "string"
    ],
    "resourceUsage": {
      "cpuSeconds": 0,
      "memoryCpuSeconds": 0,
      "instantTotalMemory": 0,
      "maxTotalMemory": 0,
      "processIO": 0,
      "processIOWaiting": 0,
      "timeInstant": "2024-01-08T17:25:16.629Z"
    }
  }
]
```

2. Program

```json
[
  {
    "programId": "string",
    "name": "string",
    "clusterId": "string",
    "version": "string",
    "installationDirectory": "string",
    "isManaged": true,
    "executablePath": "string",
    "args": [
      "string"
    ]
  }
]
```


## Rotas

As principais ações que são suportadas pela API são:

1. Submissão de novo job

2. Listar jobs existentes

3. Ler um job específico

4. Deletar um job

5. Listar os programas e as versões existentes


A seguir são demonstrados exemplos de chamadas das rotas específicas:

### Submissão de novo job (POST /jobs)

Para criar um novo job é necessário especificar alguns campos do objeto `Job`, que são obrigatórios para que a API saiba criar um novo job no gerenciador de filas. Um exemplo de corpo da requisição que atende ao requisito da rota é:

```json

    {
        "jobId": null,
        "status": null,
        "name": null,
        "startTime": null,
        "lastStatusUpdateTime": null,
        "clusterId": "1",
        "workingDirectory": "/home/pem/estudos/teste_post",
        "reservedSlots": 64,
        "scriptFile": "/home/pem/rotinas/tuber/jobs/mpi_newave.job",
        "args": ["64"],
        "resourceUsage": null
    }

```

Realizando uma requisição POST para a rota específica, com este corpo, a `hpc-queue-api` irá interagir com o gerenciador de filas em questão para criar um novo `Job` e irá monitorar este até que acabe.

São feitas as seguintes validações (podem mudar dependendo do gerenciador):

1. O conteúdo de `workingDirectory` não é vazio.

2. O conteúdo de `reservedSlots` não é vazio.

3. O conteúdo de `scriptFile` não é vazio.

É atribuído ao job criado um nome padrão, caso não seja fornecido nenhum, com o nome do diretório de execução.

### Listar jobs existentes (GET /jobs)

Ao listar os jobs existentes é retornada uma lista com todos os objetos `Job` construídos pela API por meio da submissão de novos jobs. Um exemplo de retorno é:

```json

[
    {
        "jobId": "141",
        "status": "RUNNING",
        "name": "NEWAVE-v28.16.4_micropen",
        "startTime": "2024-01-11T08:15:53",
        "lastStatusUpdateTime": "2024-01-11T11:42:29.224496",
        "endTime": null,
        "clusterId": "1",
        "workingDirectory": null,
        "reservedSlots": 64,
        "scriptFile": null,
        "args": null,
        "resourceUsage": null
    },
    {
        "jobId": "155",
        "status": "RUNNING",
        "name": "NEWAVE-v28.16.4",
        "startTime": "2024-01-11T09:21:38",
        "lastStatusUpdateTime": "2024-01-11T11:42:29.224622",
        "endTime": null,
        "clusterId": "1",
        "workingDirectory": null,
        "reservedSlots": 64,
        "scriptFile": null,
        "args": null,
        "resourceUsage": null
    }
]

```

Ao listar todos os jobs alguns campos são retornados como nulos, como o `resourceUsage`. Isto é feito para economia no tempo de processamento e do tráfego de dados. Porém, os dados existem na API e são retornados quando é feita uma leitura específica de um job.


### Ler um job específico (GET /jobs/:jobId)

Ao ler um job específico, fornecendo o `jobId` desejado, é retornado apenas o objeto `Job` em questão. Por exemplo, para o mesmo job `155` listado anteriormente:


```json
{
    "jobId": "155",
    "status": "RUNNING",
    "name": "NEWAVE-v28.16.4",
    "startTime": "2024-01-11T09:21:30",
    "lastStatusUpdateTime": "2024-01-11T11:45:34.998551",
    "endTime": null,
    "clusterId": "1",
    "workingDirectory": "/home/pem/estudos/CPAMP/Ciclo_2023-2024/Backtest/casos/ree/2020_04_rv0/newave",
    "reservedSlots": 64,
    "scriptFile": "/home/pem/rotinas/tuber/jobs/mpi_newave.job",
    "args": [
        "28.16.4",
        "64"
    ],
    "resourceUsage": {
        "cpuSeconds": 348778.644246,
        "memoryCpuSeconds": 1525763.913496,
        "instantTotalMemory": 75.47813415527344,
        "maxTotalMemory": 4139.649471282959,
        "processIO": 9649.60019,
        "processIOWaiting": 0.0,
        "timeInstant": "2024-01-11T11:45:34.998492"
    }
}
```


### Deletar um job (DELETE /jobs/:jobId)

A deleção de um job é feita através da mesma rota para a leitura de um job específico, porém com o verbo DELETE. Antes de realizar a deleção, a API valida se o job está em execução e, em caso positivo, esté é interrompido.

O retorno desta requisição é um JSON simples, e o sucesso ou não deve ser obtido a partir do `STATUS CODE` da resposta (202 para sucesso).


### Listar programas e versões existentes (GET /programs)

É possível listar os programas e versões existentes no cluster em questão, para auxiliar na submissão das rodadas de modelos energéticos por meio da [hpc-model-api](https://github.com/rjmalves/hpc-model-api). Ao se listar os programas existentes, é retornado um objeto do formato:

```json
[
    {
        "programId": "NW0",
        "name": "NEWAVE",
        "clusterId": "1",
        "version": "v28.16.4",
        "installationDirectory": "/home/pem/versoes/NEWAVE/v28.16.4",
        "isManaged": true,
        "executablePath": "/home/pem/rotinas/tuber/jobs/mpi_newave.job 28.16.4",
        "args": [
            "N_PROC"
        ]
    },
    {
        "programId": "NW1",
        "name": "NEWAVE",
        "clusterId": "1",
        "version": "v28.6.2",
        "installationDirectory": "/home/pem/versoes/NEWAVE/v28.6.2",
        "isManaged": true,
        "executablePath": "/home/pem/rotinas/tuber/jobs/mpi_newave.job 28.6.2",
        "args": [
            "N_PROC"
        ]
    }
]
```

Esta rota aceita parâmetros opcionais de query para filtrar pelo nome do modelo ou pela versão desejada. Por exemplo:


1. `GET /programs/?version=v31.21`

```json
[
    {
        "programId": "DC1",
        "name": "DECOMP",
        "clusterId": "1",
        "version": "v31.21",
        "installationDirectory": "/home/pem/versoes/DECOMP/v31.21",
        "isManaged": true,
        "executablePath": "/home/pem/rotinas/tuber/jobs/mpi_decomp.job 31.21",
        "args": [
            "N_PROC"
        ]
    }
]
```

2. `GET /programs/?name=NEWAVE

```json
[
    {
        "programId": "NW0",
        "name": "NEWAVE",
        "clusterId": "1",
        "version": "v28.16.4",
        "installationDirectory": "/home/pem/versoes/NEWAVE/v28.16.4",
        "isManaged": true,
        "executablePath": "/home/pem/rotinas/tuber/jobs/mpi_newave.job 28.16.4",
        "args": [
            "N_PROC"
        ]
    },
    {
        "programId": "NW1",
        "name": "NEWAVE",
        "clusterId": "1",
        "version": "v28.6.2",
        "installationDirectory": "/home/pem/versoes/NEWAVE/v28.6.2",
        "isManaged": true,
        "executablePath": "/home/pem/rotinas/tuber/jobs/mpi_newave.job 28.6.2",
        "args": [
            "N_PROC"
        ]
    }
]
```
