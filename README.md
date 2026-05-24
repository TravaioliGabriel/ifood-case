# iFood Case - Arquiteto de Dados

## Visão Geral
Este projeto implementa um pipeline completo de dados para ingestão, transformação e análise de dados de corridas do NYC Yellow e Green Taxi de janeiro a maio de 2023, seguindo o padrão da Arquitetura Medallion (Bronze → Silver → Gold).

O projeto utiliza **Declarative Automation Bundles (DABs)** para orquestração e implantação automatizada de todo o pipeline de dados como código.

## Arquitetura

### Camadas de Dados (Medallion Architecture)
1. **Camada Bronze** (`bronze.taxidata`): Ingestão de dados brutos do S3
   - Fontes: 
     - NYC TLC Yellow Taxi Trip Records (~16.2M registros)
     - NYC TLC Green Taxi Trip Records (~340K registros)
   - Formato: Arquivos Parquet do S3
   - Período: Janeiro - Maio 2023
   - Dados preservados em estado original para auditoria
   - Inclui timestamp de ingestão

2. **Camada Silver** (`silver.taxidata`): Dados limpos e padronizados
   - Colunas renomeadas para nomes business-friendly
   - Valores nulos removidos via `.dropna()`
   - Filtro de período aplicado (2023-01-01 a 2023-05-31)
   - Tipos de dados padronizados
   - Dados prontos para análise
   - Yellow: ~15.8M registros | Green: ~317K registros

3. **Camada Gold** (`gold.taxidata`): Dados consolidados para análise de negócio
   - Colunas essenciais selecionadas para análise
   - Schema simplificado focado em KPIs principais
   - Dados otimizados para consultas analíticas
   - Yellow: ~15.8M registros | Green: ~317K registros

### Orquestração com DABs
O projeto utiliza Declarative Automation Bundles para:
- **Implantação automatizada** de notebooks e jobs
- **Orquestração de tarefas** com dependências (Bronze → Silver → Gold)
- **Gerenciamento de ambientes** (prod/dev)
- **Versionamento** de configurações como código

## Estrutura do Projeto

```
ifood-case/
├── databricks.yml                    # Configuração principal do Bundle
├── resources/
│   └── medallion.job.yml            # Definição do Job orquestrado
├── src/
│   ├── bronze ingestion.ipynb       # Ingestão de dados do S3 para Bronze
│   ├── silver transformation.ipynb  # Transformação Bronze → Silver
│   └── gold transformation.ipynb    # Consolidação Silver → Gold
├── analysis/
│   └── analysis.ipynb               # Respostas às questões do case
├── README.md                         # Este arquivo
└── requirements.txt                  # Dependências Python
```

## Tecnologias Utilizadas
- **Plataforma**: Databricks Community Edition
- **Armazenamento**: AWS S3 (landing zone)
- **Formato de Dados**: Delta Lake
- **Processamento**: PySpark + Databricks SQL
- **Catálogo**: Unity Catalog (schemas bronze, silver, gold)
- **Orquestração**: Declarative Automation Bundles (DABs)
- **CI/CD**: Databricks CLI + Bundle deployment

## Pipeline de Dados

### 1. Camada Bronze - Ingestão de Dados
**Notebook**: `src/bronze ingestion.ipynb`

**Processo de Ingestão**:
```python
# Lê todos os arquivos parquet do S3 e consolida
file_list = dbutils.fs.ls("s3://trip-records-taxi-data/yellow_taxi/")
parquet_files = [f.path for f in file_list if f.path.endswith('.parquet')]

# União de todos os meses com padronização de tipos
df_combined = spark.read.parquet(parquet_files[0])
for file_path in parquet_files[1:]:
    df_temp = spark.read.parquet(file_path)
    df_combined = df_combined.unionByName(df_temp, allowMissingColumns=True)

# Adiciona timestamp de ingestão
df_combined = df_combined.withColumn("ingestion_timestamp", current_timestamp())

# Salva em Delta Lake
df_combined.write.format("delta").mode("overwrite").saveAsTable("bronze.taxidata.yellow_taxi_data")
```

**Tabelas Criadas**:
- `bronze.taxidata.yellow_taxi_data`: 16.186.386 registros
- `bronze.taxidata.green_taxi_data`: 339.630 registros

**Colunas Bronze** (formato original):
- VendorID, passenger_count, trip_distance
- tpep_pickup_datetime, tpep_dropoff_datetime (yellow)
- lpep_pickup_datetime, lpep_dropoff_datetime (green)
- PULocationID, DOLocationID
- fare_amount, total_amount, payment_type
- E outras 10+ colunas específicas de cada tipo

### 2. Camada Silver - Transformação de Dados
**Notebook**: `src/silver transformation.ipynb`

**Transformações aplicadas**:
- Renomeação de colunas (nomes business-friendly)
- Remoção de valores nulos (`.dropna()`)
- Filtro de período (Jan-Mai 2023)
- Padronização de tipos de dados
- Validação de qualidade

**Tabelas Criadas**:
- `silver.taxidata.yellow_taxi_data`: 15.757.617 registros
- `silver.taxidata.green_taxi_data`: 316.725 registros

**Colunas Silver** (padronizadas):
- `id_vendor`: Identificador do fornecedor
- `pickup_datetime`: Data/hora de início da corrida
- `dropoff_datetime`: Data/hora de fim da corrida
- `nr_passenger`: Número de passageiros
- `distance`: Distância percorrida
- `id_ratecode`: Código de tarifa
- `fl_store_fwd`: Flag store and forward
- `id_pickup`: ID da localização de pickup
- `id_dropoff`: ID da localização de dropoff
- `cd_payment`: Código do método de pagamento
- `vl_fare`: Valor da tarifa base
- `vl_extra`: Valores extras
- `vl_tax`: Impostos
- `vl_tip`: Gorjeta
- `vl_tolls`: Pedágios
- `surcharge`: Sobretaxa de melhoria
- `vl_total`: Valor total da corrida
- `vl_surcharge_congestion`: Sobretaxa de congestionamento
- `vl_airport_fee`: Taxa aeroportuária (yellow only)

### 3. Camada Gold - Dados Consolidados para Análise
**Notebook**: `src/gold transformation.ipynb`

**Transformações aplicadas**:
- Seleção de colunas essenciais para análise de negócio
- Renomeação final para nomenclatura business-friendly
- Schema simplificado focado em KPIs

**Tabelas Criadas**:
- `gold.taxidata.yellow_taxi_data`: 15.757.617 registros
- `gold.taxidata.green_taxi_data`: 316.725 registros

**Colunas Gold** (essenciais):
- `id_vendor`: Identificador do fornecedor
- `dh_inicio_corrida`: Data/hora de início
- `dh_final_corrida`: Data/hora de fim
- `vl_total_corrida`: Valor total da corrida
- `nr_passageiros`: Número de passageiros

## Como Executar

### Pré-requisitos
- Conta Databricks Community Edition
- Acesso ao bucket S3: `s3://trip-records-taxi-data/`
- Databricks CLI instalado (para deployment do Bundle)

### Opção 1: Execução com DABs (Recomendado)

#### 1. Deploy do Bundle
```bash
# Validar configuração
databricks bundle validate --target prod

# Deploy no workspace
databricks bundle deploy --target prod
```

#### 2. Executar o Job Orquestrado
```bash
# Executar pipeline completo (Bronze → Silver → Gold)
databricks bundle run medallion_pipeline --target prod
```

O job executará automaticamente as 3 tarefas em sequência:
1. `bronze_ingestion`: Ingestão de Yellow e Green taxi do S3
2. `silver_transformation`: Limpeza, padronização e filtros
3. `gold_transformation`: Consolidação para análise de negócio

#### 3. Executar Análise
- Abrir `analysis/analysis.ipynb`
- Executar todas as células para ver as respostas

### Opção 2: Execução Manual (Passo a Passo)

#### 1. Criar Schemas no Unity Catalog
```sql
CREATE SCHEMA IF NOT EXISTS bronze.taxidata;
CREATE SCHEMA IF NOT EXISTS silver.taxidata;
CREATE SCHEMA IF NOT EXISTS gold.taxidata;
```

#### 2. Executar Ingestão Bronze
- Abrir `src/bronze ingestion.ipynb`
- Executar todas as células sequencialmente
- Verificar: ~16.5M registros carregados (yellow + green)

#### 3. Executar Transformação Silver
- Abrir `src/silver transformation.ipynb`
- Executar todas as células sequencialmente
- Verificar: ~16.1M registros limpos com colunas business-friendly

#### 4. Executar Consolidação Gold
- Abrir `src/gold transformation.ipynb`
- Executar todas as células sequencialmente
- Verificar: 2 tabelas gold criadas (yellow + green)

#### 5. Executar Análise
- Abrir `analysis/analysis.ipynb`
- Executar todas as células para ver as respostas

## Configuração do Bundle

### databricks.yml
O arquivo principal do Bundle define:
- **Nome do projeto**: `ifood-taxi-analytics`
- **Variáveis**: catalog, cluster_node_type
- **Ambientes**: prod (production mode)
- **Recursos**: Inclui todos os arquivos `.yml` da pasta `resources/`

### Job Orquestrado (medallion.job.yml)
O job é configurado com:
- **3 tarefas** com dependências sequenciais
- **Retry automático**: 2 tentativas por tarefa
- **Timeout**: 1 hora por tarefa e total
- **Fila habilitada**: Para controle de concorrência
- **Tags**: environment, project, architecture

**Fluxo de Execução**:
```
bronze_ingestion (Yellow + Green)
        ↓
silver_transformation (Limpeza + Filtros)
        ↓
gold_transformation (Consolidação)
```
![image_1779660943776.png](./image_1779660943776.png "image_1779660943776.png")

## Principais Decisões de Design

### 1. Arquitetura Medallion
- **Bronze**: Preserva dados brutos para auditoria e replay
- **Silver**: Dados limpos e consultáveis para analistas
- **Gold**: Dados consolidados com schema simplificado para análise de negócio

### 2. Formato Delta Lake
- Transações ACID
- Capacidades de time travel
- Suporte à evolução de schema
- Performance superior ao Parquet
- Compressão automática

### 3. Unity Catalog
- Gerenciamento centralizado de metadados
- Organização por schemas (bronze/silver/gold)
- Governança de dados embutida
- Linhagem de dados rastreável

### 4. Declarative Automation Bundles (DABs)
- **Infraestrutura como código**: Toda configuração versionada
- **Deployment automatizado**: CI/CD simplificado
- **Ambientes isolados**: Prod/dev separados
- **Orquestração nativa**: Dependências entre tarefas
- **Reprodutibilidade**: Deploy consistente em qualquer ambiente

### 5. Convenção de Nomenclatura de Colunas
- `id_*`: Identificadores
- `vl_*`: Campos de valor/montante
- `nr_*`: Contagens numéricas
- `fl_*`: Flags booleanos
- `cd_*`: Códigos
- `dh_*`: Data/hora (datetime)

### 6. Processamento de Múltiplas Fontes
- Yellow e Green taxi processados separadamente até Silver
- União (UNION) na camada de análise quando necessário
- Preservação das diferenças de schema entre tipos de táxi
- `allowMissingColumns=True` para flexibilidade no Bronze

## Verificações de Qualidade de Dados

- ✅ Todas as colunas requeridas presentes na camada Silver
- ✅ Valores nulos removidos da camada Silver (`.dropna()`)
- ✅ Tipos de dados validados e convertidos no Bronze
- ✅ Intervalo de tempo verificado (Jan-Mai 2023)
- ✅ Contagens de registros validadas entre camadas:
  - Bronze → Silver: ~3% perda (dados nulos + fora do período)
  - Silver → Gold: 100% preservação
- ✅ Pipeline executado end-to-end com sucesso
- ✅ Timestamp de ingestão adicionado no Bronze

## Volume de Dados Processados

| Camada | Yellow Taxi | Green Taxi | Total |
|--------|-------------|------------|-------|
| **Bronze** | 16.186.386 | 339.630 | 16.526.016 |
| **Silver** | 15.757.617 | 316.725 | 16.074.342 |
| **Gold** | 15.757.617 | 316.725 | 16.074.342 |

**Perda de dados Bronze → Silver**: ~450K registros (2.7%)
- Motivo: Valores nulos removidos + registros fora do período Jan-Mai 2023

## Queries de Exemplo

### Receita Total por Mês (Yellow Taxi)
```sql
SELECT 
  month(dh_inicio_corrida) AS mes,
  SUM(vl_total_corrida) AS receita_total
FROM gold.taxidata.yellow_taxi_data
GROUP BY mes
ORDER BY mes;
```

### Comparação Yellow vs Green Taxi
```sql
SELECT 
  'Yellow' AS tipo_taxi,
  COUNT(*) AS total_corridas,
  AVG(vl_total_corrida) AS ticket_medio,
  AVG(nr_passageiros) AS passageiros_medio
FROM gold.taxidata.yellow_taxi_data

UNION ALL

SELECT 
  'Green' AS tipo_taxi,
  COUNT(*) AS total_corridas,
  AVG(vl_total_corrida) AS ticket_medio,
  AVG(nr_passageiros) AS passageiros_medio
FROM gold.taxidata.green_taxi_data;
```

### Distribuição de Corridas por Fornecedor
```sql
SELECT 
  id_vendor,
  COUNT(*) AS total_corridas,
  ROUND(AVG(vl_total_corrida), 2) AS ticket_medio
FROM gold.taxidata.yellow_taxi_data
GROUP BY id_vendor
ORDER BY total_corridas DESC;
```

## Resultados do Pipeline

O pipeline executou com sucesso:
- ✅ Ingestão de 16.5M+ viagens de táxi do S3 (Yellow + Green)
- ✅ Transformação de dados em 3 camadas (Bronze → Silver → Gold)
- ✅ Criação de 6 tabelas Delta Lake (2 por camada)
- ✅ Resposta às duas questões de negócio com insights acionáveis
- ✅ Preservação de todas as colunas requeridas em cada camada
- ✅ Utilização de PySpark para processamento distribuído
- ✅ Orquestração automatizada via DABs
- ✅ Validação de qualidade de dados em cada etapa

## Comandos Úteis do Bundle

```bash
# Validar configuração
databricks bundle validate --target prod

# Deploy no workspace
databricks bundle deploy --target prod

# Executar job
databricks bundle run medallion_pipeline --target prod

# Ver status
databricks jobs list-runs --job-id <job_id>

# Destruir recursos
databricks bundle destroy --target prod
```

## Estrutura de Arquivos S3

```
s3://trip-records-taxi-data/
├── yellow_taxi/
│   ├── yellow_tripdata_2023-01.parquet
│   ├── yellow_tripdata_2023-02.parquet
│   ├── yellow_tripdata_2023-03.parquet
│   ├── yellow_tripdata_2023-04.parquet
│   └── yellow_tripdata_2023-05.parquet
└── green_taxi/
    ├── green_tripdata_2023-01.parquet
    ├── green_tripdata_2023-02.parquet
    ├── green_tripdata_2023-03.parquet
    ├── green_tripdata_2023-04.parquet
    └── green_tripdata_2023-05.parquet
```
![image_1779659716652.png](./image_1779659716652.png "image_1779659716652.png")

## Contato

Para dúvidas ou problemas, abra uma issue neste repositório.

---
**Autor**: Gabriel Travaioli  
**Data**: Maio 2026  
**Plataforma**: Databricks Community Edition  
**Arquitetura**: Medallion (Bronze → Silver → Gold)  
**Orquestração**: Declarative Automation Bundles (DABs)  
**Volume de Dados**: ~16.5M registros processados
