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
    
    subgraph Pipeline Distribuído (PySpark)
        E[Extractor\nSpark Session Read] --> Q[Data Quality\nPySpark Validator]
        Q -- Gera Report --> R[dq_report.json\nMétricas de DQ]
        Q -- DataFrame Limpo --> T{Transformer\nRegras de Negócio}
        
        T -- Window Functions / Avg --> T1[Cálculo de Risk Score]
        T -- Window Functions / Top 3 --> T2[Cálculo Top 3 Sales]
    end
    
    subgraph Destino
        T1 --> L[Loader\nCoalesce(1).write]
        T2 --> L
        L --> O1[risk_score_por_regiao.csv]
        L --> O2[top_3_sales_recentes.csv]
    end
    
    CSV --> E
```

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
