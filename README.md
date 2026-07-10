# 🚗 Locadora de Carros - Data Pipeline

Este projeto é uma solução completa de **Engenharia de Dados** desenvolvida para realizar a extração, limpeza, transformação e carga (ELT/ETL) de dados transacionais de uma locadora de carros. 

Foi desenhado sob rigorosos padrões de qualidade, com foco em **Alta Confiabilidade**, aplicando princípios de **Programação Orientada a Objetos (OOP)** e **SOLID**. Toda a infraestrutura roda de forma conteinerizada via **Docker** e a orquestração é totalmente automatizada utilizando **Apache Airflow**.

---

## 📊 Fontes de Dados e Saídas

### 📥 A Fonte de Dados (Input)
O pipeline ingere os dados a partir de um arquivo `CSV` que contém o histórico transacional da locadora. Ele entra no sistema através do diretório `data/input/`.
As principais colunas esperadas nesse dataset são:
- `timestamp`: A data e hora exata em que a transação ocorreu.
- `transaction_type`: A natureza da transação (ex: `sale` para vendas, `rent` para aluguel).
- `receiving address`: O endereço ou identificador único do recebedor (cliente/agência).
- `amount`: O valor financeiro da transação.
- `location_region`: A região geográfica onde a transação aconteceu (ex: SP, RJ, Norte).
- `risk score`: Uma pontuação de risco atribuída à transação (numérico).

### 📤 Os Produtos de Dados (Output)
Como resultado do processamento, o pipeline disponibiliza os dados em `data/output/` em formato `.csv` e relatórios em `data/reports/`. O valor de negócio gerado se divide nos seguintes artefatos:

1. **`risk_score_por_regiao.csv` (Tabela Analítica 1)**
   - **O que é:** Uma tabela agregada que consolida o nível de risco médio por região.
   - **Lógica aplicada:** Os dados são agrupados por `location_region`. A média matemática da coluna `risk score` é calculada e a tabela final é ordenada de maneira decrescente (da região com maior risco médio para a menor).

2. **`top_3_sales_recentes.csv` (Tabela Analítica 2)**
   - **O que é:** Um ranking de alto valor focando nos 3 maiores `receiving address` recentes de vendas.
   - **Lógica aplicada:** O motor filtra exclusivamente transações onde `transaction_type` é "sale". Em seguida, para cada endereço, retém apenas o registro com o `timestamp` mais recente. Desta sub-base purificada, seleciona-se o *Top 3* em volume financeiro (`amount`).
   - **Colunas geradas:** `receiving address`, `amount` e `timestamp`.

3. **`dq_report.json` (Relatório de Conformidade)**
   - **O que é:** Um payload JSON descrevendo a saúde dos dados no momento da ingestão. Contém total de linhas, quantidade de registros rejeitados (com erro de tipagem ou ausência de dados cruciais) e a taxa percentual de qualidade da fonte original.

---

## 🔄 Condução do Processo ELT (Extract, Load, Transform)

Embora a sigla moderna seja ELT (onde o Load no DW é feito antes), a arquitetura desenhada reflete um padrão robusto de **ETL (Extract, Transform, Load)** modularizado. A condução do fluxo foi dividida em 4 interfaces fundamentais baseadas no Single Responsibility Principle (SRP):

1. **Extract (Extração):**
   - A classe `CSVExtractor` é a responsável isolada pela leitura. Ela não sabe o que é feito com os dados depois. Se amanhã os dados vierem do Amazon S3 ou de um Postgres, basta criar um `S3Extractor` respeitando a interface, e o pipeline principal continua o mesmo.

2. **Data Quality & Cleansing (Qualidade e Limpeza):**
   - O `PandasDataQualityValidator` atua como uma barreira (Gatekeeper). Ele avalia os dados do Extractor, identifica valores nulos e verifica a tipagem correta da data (`timestamp`) e números (`amount`, `risk score`). Dados corrompidos são isolados e removidos do DataFrame de trabalho, gerando a "taxa de conformidade" (Compliance Percentage) registrada no report.

3. **Transform (Transformação de Negócio):**
   - Com dados 100% limpos e íntegros, o `BusinessTransformer` assume. Este módulo não faz I/O (leitura/escrita em disco). Ele recebe um DataFrame do Pandas em memória e gera as duas tabelas-resultado requeridas (`risk_score_por_regiao` e `top_3_sales_recentes`). É aqui que a lógica pesada mora e por isso é profundamente coberta por **Testes Unitários**.

4. **Load (Carga Final):**
   - O `CSVLoader` recebe um dicionário contendo o nome e o conteúdo de cada tabela-resultado para exportar de volta ao disco de forma persistente.

---

## 🏗️ Diagramas da Arquitetura

### 1. Fluxo Funcional dos Dados (Mermaid)
Este diagrama demonstra como a informação viaja, desde a leitura até as saídas processadas:

```mermaid
flowchart TD
    subgraph Data Sources
        CSV[Arquivo CSV\n'Transações']
    end
    
    subgraph Pipeline de Dados
        E[Extractor\nLê o CSV] --> Q[Data Quality\nPandas Validator]
        Q -- Gera Report --> R[dq_report.json\nMétricas de DQ]
        Q -- Dados Limpos --> T{Transformer\nRegras de Negócio}
        
        T -- Agrupamento & Média --> T1[Cálculo de Risk Score\npor Região]
        T -- Filtros & Top 3 --> T2[Cálculo Top 3\nSales Recentes]
    end
    
    subgraph Destino
        T1 --> L[Loader\nGrava em Disco]
        T2 --> L
        L --> O1[risk_score_por_regiao.csv]
        L --> O2[top_3_sales_recentes.csv]
    end
    
    CSV --> E
```

### 2. Orquestração e Componentes (Sequence Diagram)
Este diagrama foca no motor de execução, mostrando o papel do **Apache Airflow** na condução do processo:

```mermaid
sequenceDiagram
    participant Airflow DAG
    participant Pipeline (Controller)
    participant Extractor (Ler)
    participant Validator (DQ)
    participant Transformer (Regras)
    participant Loader (Gravar)

    Airflow DAG->>Pipeline: Dispara a execução diária
    
    activate Pipeline
    Pipeline->>Extractor: extract()
    Extractor-->>Pipeline: DataFrame bruto (dados lidos)
    
    Pipeline->>Validator: validate(df_raw)
    Validator->>Reports (Disco): Salva dq_report.json
    Validator-->>Pipeline: DataFrame limpo e tipado
    
    Pipeline->>Transformer: transform_risk_score(df_clean)
    Transformer-->>Pipeline: df_risk_score (Tabela 1)
    
    Pipeline->>Transformer: transform_top_sales(df_clean)
    Transformer-->>Pipeline: df_top_sales (Tabela 2)
    
    Pipeline->>Loader: load({Tabela1, Tabela2})
    Loader->>Outputs (Disco): Salva .csv finais
    Loader-->>Pipeline: Sucesso
    
    deactivate Pipeline
    Airflow DAG-->>Usuário: Job Concluído
```

---

## 📂 Estrutura Físico-Lógica do Projeto

O código está organizado seguindo Clean Code:

```text
locadora_de_carros/
├── .github/workflows/               # 🤖 Pipeline de CI (Integração Contínua) e GitHub Actions
├── dags/                            # ⏰ Configurações do Apache Airflow
│   └── locadora_pipeline_dag.py     # Definição da DAG agendada
├── data/                            # 🗂️ Camada de Storage (Ignorado pelo Git)
│   ├── input/                       # Dataset bruto
│   ├── output/                      # Produtos de dados finais
│   └── reports/                     # Arquivos JSON gerados pela validação DQ
├── src/                             # 🧠 Core / Aplicação Principal
│   ├── interfaces/                  # Classes abstratas forçando o padrão de dependências
│   ├── extractor/                   # Implementação do motor de leitura (CSVExtractor)
│   ├── quality/                     # Validador de dados (PandasDataQualityValidator)
│   ├── transformer/                 # Lógica de agrupamento e rankings (BusinessTransformer)
│   ├── loader/                      # Motor de gravação do resultado (CSVLoader)
│   └── pipeline.py                  # "Controller" que amarra interfaces e conduz o fluxo
├── tests/                           # 🧪 Suíte de Alta Confiabilidade
│   ├── unit/                        # Testes isolados das regras de negócio
│   └── integration/                 # Testes End-to-End simulando o ciclo completo
├── docker-compose.yml               # 🐳 Infraestrutura como código (Airflow services)
├── Dockerfile                       # Imagem customizada para o Worker (Python + Pandas)
├── requirements.txt                 # Manifestações de dependências do Python
└── arquitetura_locadora.drawio      # ✏️ Representação da Arquitetura para ser aberta no diagrams.net
```

---

## 🚀 Como Rodar o Projeto

Você tem duas opções: Simular o ambiente de produção (via Airflow + Docker) ou ambiente de desenvolvimento local (para testes).

### Opção 1: Via Docker & Apache Airflow (Produção Recomendada)
Toda a orquestração do pipeline é feita em containers, garantindo facilidade de execução em qualquer máquina.

1. **Subir os serviços:** Na raiz do projeto, digite:
   ```bash
   docker-compose up -d --build
   ```
2. **Acessar o Painel de Orquestração:** Abra seu navegador no endereço `http://localhost:8080`.
3. **Login:** Utilize as credenciais -> Usuário: `airflow` | Senha: `airflow`.
4. **Acionar o Pipeline:** Procure pela DAG chamada `locadora_pipeline_dag`. Ative-a (toggle *Unpause*) e clique no botão **Trigger DAG** (ícone de "Play").
5. **Verificar os Produtos de Dados:** Os resultados processados serão gerados dentro da pasta `data/output/` e o score de qualidade em `data/reports/`.

### Opção 2: Validação Local via Linha de Comando
Se você é um desenvolvedor e deseja validar a lógica sem subir a infraestrutura completa:

1. **Configurar Ambiente Python Virtual:**
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```
2. **Rodar a Suíte de Testes (Alta Confiabilidade):**
   Nosso ambiente contempla testes unitários e de integração validando desde componentes isolados até simulações reais do banco de dados (mocks).
   ```bash
   export PYTHONPATH=$(pwd)
   pytest tests/
   ```

---

## 🤖 Integração Contínua (CI/CD) e Alta Confiabilidade

Para garantir a **Alta Confiabilidade**, este repositório está integrado nativamente ao GitHub Actions. O arquivo `.github/workflows/ci.yml` automatiza a qualidade de código sempre que existe um novo envio (push) ou uma nova proposta de código (pull_request) nas branches `develop` e `main`.

A Action no GitHub separa de forma isolada (em jobs distintos) dois fluxos principais:

1. **Unit Tests (Testes Unitários):**
   - **Como funciona:** O job `test-unit` instala o ambiente e as dependências em uma máquina virtual Linux provida pelo GitHub. Ele executa exclusivamente os testes contidos na pasta `tests/unit/`.
   - **O que avalia:** Foca microscopicamente na lógica interna das Classes. Por exemplo, valida matematicamente se o cálculo da média do Risk Score está correto ou se a lógica do Top 3 funciona (lidando com empates, etc), sem se preocupar com leitura real de arquivos ou com a orquestração final. 

2. **Integration Tests (Testes de Integração):**
   - **Como funciona:** O job `test-integration` roda em uma esteira completamente separada. Ele executa os testes localizados em `tests/integration/`.
   - **O que avalia:** Avalia o cenário de ponta-a-ponta (Macro). Um dataset fake é gerado em tempo de execução e a classe controladora `LocadoraPipeline` é instanciada e executada. O teste acompanha se o dado flui corretamente e sem atritos sistêmicos desde o `Extractor` até o `Loader`, verificando se os arquivos foram efetivamente gerados no disco com os resultados perfeitos.

---

## 🛡️ Análise de Qualidade de Dados (Erros e Anomalias)

Conforme os requisitos do desafio técnico, implementou-se uma etapa automatizada e rigorosa de Data Quality. O módulo avalia minuciosamente o CSV ingerido em busca de **valores faltantes (nulos), inconsistentes e incorretos**.

As métricas geradas são sumarizadas automaticamente no arquivo `data/reports/dq_report.json`, contendo os indicadores essenciais:
- **Quantidade de Registros:** O volume bruto de transações ingeridas.
- **Quantidade de Erros (Anomalias):** A contabilização estrita de registros que falharam na inspeção de negócios (ex: anomalias onde esperava-se um `float` mas veio uma string quebrando a consistência do sistema, ou campos de `timestamp` ausentes).
- **Valores Faltantes:** Um rastreamento mapeado coluna a coluna apontando a ausência de preenchimento de campos vitais.
- **Percentual de Conformidade:** A métrica monitorada evidenciando matematicamente a razão de incidentes (`qtd. erros / qtd. registros`), além da taxa geral de saúde da base.

Todo registro considerado inconsistente ou incorreto é reportado neste arquivo isolado e descartado do *DataFrame* que avança, blindando e imunizando o `Transformer` contra cálculos errôneos de médias ou corrupção da ordenação do ranking.

---

## 📈 Possíveis Evoluções (Next Steps)

Pensando no ciclo de vida em longo prazo e na escalabilidade deste ecossistema corporativo de dados, listamos melhorias que trariam amadurecimento ao produto:

1. **Escalabilidade Computacional (PySpark/Polars):** Dado que atualmente o Pandas é in-memory, caso o volume diário de locações da empresa atinja a casa dos Gigabytes ou Terabytes, migrar o núcleo de processamento do Transformer para `PySpark` (Distributed Processing) ou `Polars` (Multithreading em Rust) impediria crashes por falta de memória (OOM).
2. **Qualidade de Dados Estrita (Great Expectations & Data Contracts):** Substituir as lógicas manuais de qualidade na etapa do *Validator* para um framework focado como o `Great Expectations` ou forçar Data Contracts puros utilizando o `Pydantic` de maneira agressiva.
3. **Migração para Data Warehouse / Data Lake (Modern Data Stack):** Em vez de descarregar dados processados em arquivos localmente via `CSVLoader`, criar um novo injetor que mande a saída num formato colunar (`Parquet`) para um bucket na AWS (S3) catalogado pelo Glue, ou para um DW robusto como Google BigQuery / Snowflake. Dessa forma, as equipes de visualização e Business Intelligence teriam as tabelas diretamente plugadas no Power BI.
4. **Governança de Orquestração (Cloud):** Abstrair a atual subida de infraestrutura do Airflow cru em contêineres Docker para utilizá-la como Serverless ou serviço gerenciado usando o AWS MWAA (Managed Workflows for Apache Airflow) ou Google Cloud Composer, livrando as equipes de sustentar o servidor web e o banco Postgres dos metadados.
