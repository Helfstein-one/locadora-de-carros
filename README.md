# 🚗 Locadora de Carros - Data Pipeline

Este projeto é uma solução de Engenharia de Dados para o processamento, limpeza e análise de dados de transações de uma locadora de carros. Ele foi desenvolvido com foco em alta confiabilidade, aplicando princípios **SOLID**, **Programação Orientada a Objetos (OOP)** e utilizando **Docker** e **Apache Airflow** para orquestração automatizada.

## 🎯 Objetivo do Pipeline

O pipeline executa as seguintes tarefas, de forma automatizada:
1. **Importação (Extract):** Faz a ingestão do arquivo CSV de entrada contendo o histórico de transações.
2. **Qualidade de Dados & Limpeza (Quality & Cleanse):** Monitora métricas de qualidade (linhas nulas, valores inválidos, anomalias) gerando reports e limpando o dataset.
3. **Transformação (Transform):** Processa os dados limpos para criar duas tabelas-resultado específicas:
   - **Tabela 1:** Lista em ordem decrescente as `location_region` pela média de `risk score`.
   - **Tabela 2:** Lista os Top 3 `receiving address` com maior `amount` dentre as transações mais recentes (onde `transaction_type` = `sale`).
4. **Carga (Load):** Exporta os dados finais gerados no formato `.csv` (ou persiste num banco de dados, conforme configuração).

## 🏗️ Arquitetura e Componentes

A solução foi pensada de maneira modular para ser executada em qualquer ambiente (via containers) e facilitar testes unitários e extensibilidade.

```mermaid
graph TD
    A[Dataset Original CSV] --> B(Extractor)
    B --> C(Data Quality Validator)
    C -- Logs de Erros --> DQ_Report[(Relatórios de Data Quality)]
    C -- Dados Validados --> D(Transformer)
    D --> T1[Tabela 1: Risk Score por Região]
    D --> T2[Tabela 2: Top 3 Sales]
    T1 --> E(Loader)
    T2 --> E
    E --> F[(Output Final)]
    
    subgraph Orquestração (Airflow)
    B
    C
    D
    E
    end
```

### 🧩 Boas Práticas e SOLID
- **Single Responsibility Principle (SRP):** Cada módulo (`extractor.py`, `transformer.py`, `loader.py`, `quality.py`) possui uma única responsabilidade. O Transformer não sabe de onde vêm os dados, ele apenas processa DataFrames.
- **Dependency Inversion Principle (DIP):** O pipeline principal depende de abstrações (`ExtractorInterface`, `LoaderInterface`) ao invés de implementações diretas. Isso permite trocar de um CSV Extractor para um SQL Extractor no futuro sem alterar o motor do pipeline.
- **Testabilidade:** A lógica de cálculo do *Risk Score* e dos *Top 3 Sales* está isolada no Transformer, permitindo testes unitários rigorosos sem necessidade de dependências externas.

## 🛠️ Tecnologias Utilizadas
- **Python 3.11+**: Linguagem principal.
- **Pandas**: Para as transformações tabulares em memória.
- **Pydantic**: Para validação rigorosa dos tipos e dados (Data Quality).
- **Apache Airflow**: Para orquestração da DAG do pipeline de dados.
- **Docker & Docker Compose**: Para containerização e padronização do ambiente.
- **Pytest**: Para testes unitários e de integração.

## 🚀 Como Executar

### 1. Pré-requisitos
- Ter o **Docker** e o **Docker Compose** instalados na sua máquina.
- Colocar o arquivo CSV original na pasta `data/input/`.

### 2. Subindo o Ambiente (Airflow)
Na raiz do projeto, execute:
```bash
docker-compose up -d --build
```
Isso fará o build da imagem customizada (que inclui o Pandas, Pytest, Pydantic) e iniciará os serviços do Airflow (Webserver, Scheduler, Postgres).

### 3. Acessando a Orquestração
- Acesse o Airflow através do navegador em: `http://localhost:8080`
- **Usuário:** `airflow` | **Senha:** `airflow`
- Na interface web, procure pela DAG `locadora_pipeline_dag` e ative-a (botão de toggle).
- Clique em **Trigger DAG** (play) para iniciar o processamento manualmente.

### 4. Verificando os Resultados
Após a execução com sucesso:
- **Tabelas de Resultado:** Estarão disponíveis na pasta `data/output/`.
- **Relatório de Data Quality:** O arquivo contendo as métricas (linhas ignoradas, nulos, tipagem incorreta) estará disponível em `data/reports/dq_report.json`.

## 🧪 Rodando os Testes

Para garantir a confiabilidade (Alta Confiabilidade), o projeto conta com suítes de testes automatizados. Você pode executá-los através do próprio container:

```bash
# Para testes unitários
docker-compose exec airflow-webserver pytest tests/unit/

# Para testes integrados (fluxo de ponta a ponta com dados mockados)
docker-compose exec airflow-webserver pytest tests/integration/
```

## 📂 Estrutura de Pastas
```text
locadora_de_carros/
├── dags/
│   └── locadora_pipeline_dag.py     # DAG do Airflow
├── data/
│   ├── input/                       # Coloque o CSV de entrada aqui
│   ├── output/                      # Tabelas-resultado geradas
│   └── reports/                     # Métricas de Data Quality
├── src/
│   ├── interfaces/                  # Classes base e interfaces (SOLID)
│   ├── extractor/                   # Leitura de dados (CSV, etc)
│   ├── quality/                     # Validação e métricas Pydantic
│   ├── transformer/                 # Regras de negócio / agregações
│   ├── loader/                      # Persistência do resultado
│   └── pipeline.py                  # Orquestrador central em Python
├── tests/
│   ├── unit/                        # Testes isolados
│   └── integration/                 # Testes de ponta a ponta
├── docker-compose.yml               # Arquitetura de containers
├── Dockerfile                       # Imagem base Airflow com dependências
└── requirements.txt                 # Bibliotecas Python (Pandas, Pytest, Pydantic)
```
