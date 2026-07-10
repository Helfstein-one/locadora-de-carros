# 🚗 Locadora de Carros - Data Pipeline (PySpark)

Este projeto é uma solução completa de **Engenharia de Dados** desenvolvida para realizar a extração, limpeza, transformação e carga (ELT/ETL) de dados transacionais de uma locadora de carros. 

Nesta versão, a arquitetura foi evoluída para **Big Data**. Todo o motor de processamento, antes em Pandas, foi refatorado para utilizar **Apache Spark (PySpark)**, provendo paralelismo e tolerância a falhas. A infraestrutura continua conteinerizada via **Docker** e a orquestração (agora injetando e manipulando instâncias da JVM) é feita via **Apache Airflow**.

---

## 📊 Fontes de Dados e Saídas

### 📥 A Fonte de Dados (Input)
O pipeline ingere os dados a partir de um arquivo `CSV` físico e versionado neste repositório em `data/input/data.csv`. 
As principais colunas são:
- `timestamp`: A data e hora exata em que a transação ocorreu.
- `transaction_type`: A natureza da transação (ex: `sale` para vendas, `rent` para aluguel).
- `receiving address`: O endereço ou identificador único do recebedor (cliente/agência).
- `amount`: O valor financeiro da transação.
- `location_region`: A região geográfica onde a transação aconteceu (ex: SP, RJ, Norte).
- `risk score`: Uma pontuação de risco atribuída à transação (numérico).

### 📤 Os Produtos de Dados (Output)
Os dados são exportados via o método `df.coalesce(1).write.csv()` do Spark para a pasta `data/output/`.

1. **`risk_score_por_regiao.csv` (Tabela Analítica 1)**
   - Agrega o nível de risco médio por região (`location_region`).
2. **`top_3_sales_recentes.csv` (Tabela Analítica 2)**
   - Ranking de alto valor calculado de maneira distribuída utilizando **Window Functions** do PySpark particionando por `receiving address`, filtrando "sales" recentes e buscando o Top 3 financeiro (`amount`).
3. **`dq_report.json` (Relatório de Conformidade)**
   - Um payload JSON apontando total de linhas, nulos por coluna, registros rejeitados e o percentual de saúde dos dados (`qtd erros / qtd total`).

---

## 🔄 Condução do Processo ELT (SRP e PySpark)

1. **Extract (Extração):** O `CSVExtractor` recebe uma `SparkSession` e faz a leitura lazily do disco distribuído (ou local).
2. **Data Quality & Cleansing:** O `PySparkDataQualityValidator` aplica funções de tipagem restrita (`cast`) e `isnan/isNull` de forma distribuída em todo o cluster. Nulos vitais são derrubados antes de prosseguir.
3. **Transform (Transformação de Negócio):** O `BusinessTransformer` é a camada analítica com agregações e `Window Functions` pesadas rodando na JVM.
4. **Load (Carga Final):** O `SparkCSVLoader` manipula os `part-000` nativos do Hadoop gerados no output para consolidá-los em um arquivo único finalizado.

---

## 🏗️ Diagramas da Arquitetura

### 1. Fluxo Funcional dos Dados (Mermaid)
```mermaid
flowchart TD
    subgraph Data Sources
        CSV[Arquivo CSV\n'Transações']
    end
    
    subgraph Pipeline Distribuido PySpark
        E[Extractor\nSpark Session Read] --> Q[Data Quality\nPySpark Validator]
        Q -- Gera Report --> R[dq_report.json\nMétricas de DQ]
        Q -- DataFrame Limpo --> T{Transformer\nRegras de Negócio}
        
        T -- Window Functions / Avg --> T1[Cálculo de Risk Score]
        T -- Window Functions / Top 3 --> T2[Cálculo Top 3 Sales]
    end
    
    subgraph Destino
        T1 --> L[Loader\nCoalesce File]
        T2 --> L
        L --> O1[risk_score_por_regiao.csv]
        L --> O2[top_3_sales_recentes.csv]
    end
    
    CSV --> E
```

### 2. Orquestração e Ciclo de Vida da JVM (Sequence Diagram)
Este diagrama foca na injeção de dependência e no controle da **SparkSession**, provando a orquestração ponta a ponta:

```mermaid
sequenceDiagram
    participant DAG as Airflow DAG / Local Runner
    participant Pipeline as LocadoraPipeline
    participant Spark as SparkSession (JVM)
    participant Extractor
    participant Validator
    participant Transformer
    participant Loader

    DAG->>Spark: Inicializa Sessão Spark (Master Local/Cluster)
    DAG->>Pipeline: Injeta Interfaces e Sessão
    
    activate Pipeline
    Pipeline->>Extractor: extract()
    Extractor-->>Pipeline: PySpark DataFrame (Lazy Read)
    
    Pipeline->>Validator: validate(df_raw)
    Note over Validator: Execução Distribuída:<br/>df.withColumn(...cast())<br/>Check de nulos e isnan
    Validator->>Relatórios: Grava dq_report.json local
    Validator-->>Pipeline: DataFrame tipado e purificado
    
    Pipeline->>Transformer: transform_risk_score(df_clean)
    Note over Transformer: df.groupBy(...).agg(avg(...))
    Transformer-->>Pipeline: DataFrame de Risk Score
    
    Pipeline->>Transformer: transform_top_sales(df_clean)
    Note over Transformer: Window.partitionBy(...).orderBy(...)
    Transformer-->>Pipeline: DataFrame Top 3
    
    Pipeline->>Loader: load(dataframes_dict)
    Note over Loader: df.coalesce(1).write.csv()
    Loader->>Outputs: Salva na pasta data/output/
    Loader-->>Pipeline: OK
    
    Pipeline->>Spark: spark.stop() (Encerra JVM)
    deactivate Pipeline
    DAG-->>Usuário: Job Airflow Finalizado
```

### Explicação Granular de Cada Etapa (PySpark Engine):
1. **Extractor (Leitura Distribuída):** O `CSVExtractor` não carrega o arquivo para a memória RAM bruta. Ele cria um ponteiro virtual (Lazy Evaluation) apontando para o disco via `spark.read.csv()`, inferindo o esquema nativamente. Se amanhã houver 1000 arquivos CSVs, ele lê todos simultaneamente em cluster.
2. **Validator (Quality & Cleanse):** Recebe o DataFrame PySpark e executa tipagem (`cast`) massiva em paralelo nas *tasks*. Através da API de colunas, verifica nulos e gera a "Taxa de Conformidade" exportada no Json. O *Drop* nativo isola transações sujas e salva a matemática financeira.
3. **Transformer (Business Logic):** Coração analítico. Não há uso de loopings custosos (`for`). 
   - Na Tabela 1, usamos `df.groupBy().agg(avg())`, delegando o cálculo da média por região pros nós de processamento da JVM.
   - Na Tabela 2, o ranqueamento de "vendas mais recentes" é solucionado sem gargalos utilizando `pyspark.sql.window.Window`. A base é particionada em memória RAM virtual por `receiving address`, filtrada pela linha mais nova e em seguida submetida a um limitador universal (Top 3) utilizando as otimizações do *Catalyst Optimizer* do Spark.
4. **Loader (Persistência):** O `SparkCSVLoader` recebe o ponteiro do trabalho finalizado. Como o Spark salva dados em dezenas de minúsculos arquivos (`part-000x`), utilizamos `.coalesce(1)` para forçar a junção no último nó e descarregar um único arquivo `.csv` final na pasta `data/output/`.

---

## 🚀 Como Rodar o Projeto

Nesta evolução focamos na flexibilidade. Você tem três maneiras de acionar este motor:

### Opção 1: Via Docker & Apache Airflow (Produção Recomendada)
Toda a orquestração ocorre isolada em contêineres. O Dockerfile instala o `default-jre-headless` (Java) e o `pyspark`, subindo o pipeline perfeitamente.
1. Na raiz, digite:
   ```bash
   docker-compose up -d --build
   ```
2. Abra seu navegador em `http://localhost:8080` (User: `airflow`, Pass: `airflow`).
3. Ative a DAG `locadora_pipeline_dag` e clique no "Play".

### Opção 2: Script Nativo PySpark (Desenvolvimento Simples Local)
Se você quer simular o processamento sem instalar o Docker ou se perder no Airflow, criei um script focado em instanciar a JVM local e rodar.
1. Crie seu ambiente local e instale as dependências:
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```
2. Execute o script nativo:
   ```bash
   python3 run_local_spark.py
   ```
*O script levantará a `SparkSession` na sua máquina, processará e guardará os arquivos no `/data`.*

### Opção 3: Suíte de Testes (Alta Confiabilidade e CI/CD)
O repositório é validado na nuvem via **GitHub Actions** em duas frentes independentes baseadas em Fixtures universais de PySpark (`tests/conftest.py`):
1. **Unit Tests (`test-unit`):** Cria-se DataFrames sintéticos mockando as lógicas matemáticas sem I/O real.
2. **Integration Tests (`test-integration`):** Uma orquestração full End-to-End simulando as saídas no disco.

Para rodá-los na sua máquina:
```bash
export PYTHONPATH=$(pwd)
pytest tests/
```
