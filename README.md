# TCC UNIFOR — Pipeline de Dados do Mercado Pago

Projeto desenvolvido como parte do Trabalho de Conclusão de Curso (TCC), com o objetivo de implementar um pipeline de engenharia de dados utilizando Databricks.

A solução realiza a ingestão, transformação, validação de qualidade e disponibilização dos dados seguindo uma arquitetura em camadas.

## Arquitetura

O pipeline foi estruturado seguindo o padrão Medallion Architecture:

API / Fonte de Dados
        ↓
    Ingestão
        ↓
     Bronze
        ↓
     Silver
        ↓
 Validação de Qualidade
        ↓
      Gold

### Bronze

Responsável pela ingestão e armazenamento inicial dos dados provenientes da fonte, preservando os dados com o mínimo de transformação.

Principais processos:

- `01_ingestao_mercado_pago.py`
- `02_bronze_mercado_pago.py`

### Silver

Responsável pelo tratamento, padronização e preparação dos dados para consumo.

Principais processos:

- `03_silver_mercado_pago.py`
- `04_qualidade_silver.py`

A etapa de qualidade realiza validações sobre os dados processados antes da disponibilização na camada Gold.

### Gold

Responsável pela disponibilização dos dados tratados e preparados para análise e consumo.

Principal processo:

- `05_gold_mercado_pago.py`

## Orquestração

A execução do pipeline é realizada através de um Databricks Job.

As tarefas possuem dependências sequenciais:

`01_ingestao_mercado_pago`
↓
`02_bronze_mercado_pago`
↓
`03_silver_mercado_pago`
↓
`04_qualidade_silver`
↓
`05_gold_mercado_pago`

O Job está configurado para execução automática utilizando o scheduler do Databricks.

## Databricks Asset Bundle

A configuração da infraestrutura e da orquestração também é mantida como código.

Estrutura:

```text
TCC_UNIFOR/
├── databricks.yml
├── resources/
│   └── mercado_pago_job.yml
├── src/
│   ├── bronze/
│   │   ├── 01_ingestao_mercado_pago.py
│   │   └── 02_bronze_mercado_pago.py
│   ├── silver/
│   │   ├── 03_silver_mercado_pago.py
│   │   └── 04_qualidade_silver.py
│   └── gold/
│       └── 05_gold_mercado_pago.py
└── README.md