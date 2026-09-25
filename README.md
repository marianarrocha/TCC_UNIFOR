# Plataforma de Dados para Gestão Financeira de Pequenas Empresas

Projeto desenvolvido como Trabalho de Conclusão de Curso (TCC) da Universidade de Fortaleza (UNIFOR).

A solução propõe uma arquitetura de Engenharia de Dados para centralizar informações financeiras provenientes de diferentes meios e plataformas de pagamento, permitindo que pequenas empresas acompanhem seus dados em um único ambiente por meio de dashboards e recursos de Inteligência Artificial.

---

- ## Sobre o projeto

Pequenas empresas podem utilizar diferentes meios para realizar vendas e receber pagamentos, como PIX, cartões, maquininhas, carteiras digitais e plataformas como o Mercado Pago.

Nesse cenário, as informações financeiras acabam distribuídas entre diferentes sistemas, aplicativos e relatórios. Isso pode dificultar a consolidação dos dados e a obtenção de uma visão única sobre os recebimentos da empresa.

Este projeto busca solucionar esse problema através da construção de uma plataforma de dados capaz de integrar diferentes fontes de pagamento, processar e padronizar essas informações e disponibilizá-las em uma camada centralizada para análise e consulta.

A proposta é permitir que o mesmo conjunto de dados possa ser utilizado tanto para análises visuais em dashboards quanto para consultas através de um agente de Inteligência Artificial.

---

- ## Problema

Quando uma empresa utiliza diferentes plataformas financeiras, cada uma possui sua própria estrutura de dados, histórico de transações e forma de disponibilização das informações.

Essa fragmentação pode dificultar atividades como:

- acompanhar os recebimentos de forma consolidada;
- visualizar o volume financeiro total;
- comparar diferentes fontes de pagamento;
- acompanhar a evolução das transações;
- identificar tendências nos dados;
- gerar indicadores financeiros;
- consultar rapidamente informações relevantes;
- utilizar os dados como apoio à tomada de decisão.

Além disso, a consolidação manual dessas informações pode exigir a utilização de planilhas e relatórios separados, aumentando a complexidade do processo.

---

- ## Solução proposta

A solução utiliza conceitos de Engenharia de Dados para construir um fluxo automatizado responsável por:

1. coletar dados das diferentes fontes;
2. armazenar os dados recebidos;
3. realizar tratamentos e padronizações;
4. executar validações de qualidade;
5. consolidar as informações;
6. disponibilizar os dados para diferentes formas de consumo.

A arquitetura pode ser representada pelo seguinte fluxo:

```text
        Fontes de Pagamento
               │
               ▼
            Ingestão
               │
               ▼
            Bronze
               │
               ▼
            Silver
               │
               ▼
      Validação de Qualidade
               │
               ▼
             Gold
               │
        ┌──────┴──────┐
        ▼             ▼
    Dashboard     Agente de IA
```

Dessa forma, novas fontes de dados podem ser incorporadas à arquitetura mantendo uma estrutura centralizada de processamento e consumo.

---

- ## Arquitetura de dados

O projeto utiliza uma arquitetura baseada no conceito de **Medallion Architecture**, separando o processamento dos dados em diferentes camadas.

### 🥉 Bronze

A camada Bronze é responsável pela entrada e persistência inicial dos dados provenientes das fontes.

Nesta etapa, o objetivo é manter os dados próximos de sua estrutura original, criando uma camada de armazenamento que permita rastreabilidade e processamento posterior.

No fluxo atualmente implementado para o Mercado Pago:

```text
01_ingestao_mercado_pago.py
        ↓
02_bronze_mercado_pago.py
```

---

### 🥈 Silver

A camada Silver é responsável pelo tratamento e padronização dos dados.

Nesta etapa podem ser realizadas operações como:

- tratamento de tipos de dados;
- padronização de campos;
- tratamento de valores inválidos;
- remoção ou tratamento de inconsistências;
- aplicação de regras de negócio;
- preparação dos dados para consumo.

Processo:

```text
03_silver_mercado_pago.py
```

---

- ## Qualidade dos dados

Após o processamento da camada Silver, o pipeline possui uma etapa específica para validação da qualidade dos dados.

Processo:

```text
04_qualidade_silver.py
```

Essa etapa tem como objetivo verificar se os dados processados atendem às regras e critérios definidos antes de serem disponibilizados para a camada Gold.

A inclusão de uma etapa de qualidade ajuda a aumentar a confiabilidade das informações utilizadas posteriormente pelos dashboards e demais consumidores.

---

- ### Gold

A camada Gold representa os dados preparados para consumo.

Processo:

```text
05_gold_mercado_pago.py
```

Nesta camada, os dados já passaram pelas etapas de ingestão, tratamento e validação, podendo ser utilizados para construção de indicadores, análises e outras aplicações.

A Gold funciona como uma das principais interfaces entre a Engenharia de Dados e as aplicações que utilizam essas informações.

---

- ## Dashboard

Uma das formas de consumo previstas pela solução é através de dashboards.

O dashboard permite transformar os dados processados em indicadores e visualizações que facilitem o acompanhamento das informações financeiras.

A proposta é permitir que o responsável pela empresa consiga consultar informações consolidadas sem precisar acessar individualmente cada plataforma de pagamento.

A partir da camada Gold, podem ser construídos indicadores relacionados a:

- volume financeiro;
- quantidade de transações;
- evolução dos recebimentos;
- distribuição por fonte de pagamento;
- comportamento das transações ao longo do tempo;
- outros indicadores relevantes para o negócio.

---

- ## Inteligência Artificial

Além da visualização através de dashboards, o projeto prevê a utilização de Inteligência Artificial como uma nova forma de interação com os dados.

A proposta é permitir que o usuário realize perguntas utilizando linguagem natural.

Exemplos:

```text
"Quanto recebi neste mês?"

"Qual foi o período com maior volume de pagamentos?"

"Como os recebimentos evoluíram nos últimos meses?"

"Qual fonte de pagamento possui maior participação?"
```

O agente de IA utiliza os dados consolidados pela plataforma para auxiliar na consulta e interpretação das informações.

Dessa forma, o usuário passa a ter duas formas principais de interação:

```text
                 Dados Gold
                     │
             ┌───────┴───────┐
             │               │
             ▼               ▼
         Dashboard       Agente de IA
             │               │
             ▼               ▼
       Visualização      Linguagem Natural
```

---

- ## Orquestração

A execução do pipeline é automatizada através de um **Databricks Job**.

O workflow atualmente possui cinco etapas executadas sequencialmente:

```text
01_ingestao_mercado_pago
          │
          ▼
02_bronze_mercado_pago
          │
          ▼
03_silver_mercado_pago
          │
          ▼
04_qualidade_silver
          │
          ▼
05_gold_mercado_pago
```

Cada etapa possui dependência da execução bem-sucedida da etapa anterior.

Isso permite automatizar todo o fluxo desde a ingestão até a disponibilização dos dados tratados.

O Job também possui agendamento configurado no Databricks, permitindo a execução automática do pipeline.

---

- ##  Databricks Asset Bundles

Além do código responsável pelo processamento dos dados, a configuração do workflow também é mantida como código.

O projeto utiliza **Databricks Asset Bundles** para versionar as configurações relacionadas à execução do pipeline.

O arquivo:

```text
databricks.yml
```

contém a configuração principal do Bundle.

A definição do Job está localizada em:

```text
resources/mercado_pago_job.yml
```

Essa abordagem permite manter no mesmo repositório:

- código de Engenharia de Dados;
- configuração das tarefas;
- dependências entre as etapas;
- agendamento;
- configuração do ambiente;
- definição do workflow.

Assim, alterações realizadas tanto no processamento quanto na orquestração podem ser controladas através do Git.

---

- ##  Estrutura do projeto

A estrutura principal do projeto está organizada da seguinte forma:

```text
TCC_UNIFOR/
│
├── databricks.yml
├── README.md
│
├── resources/
│   └── mercado_pago_job.yml
│
└── src/
    │
    ├── bronze/
    │   ├── 01_ingestao_mercado_pago.py
    │   └── 02_bronze_mercado_pago.py
    │
    ├── silver/
    │   ├── 03_silver_mercado_pago.py
    │   └── 04_qualidade_silver.py
    │
    └── gold/
        └── 05_gold_mercado_pago.py
```

---

- ## Fluxo de processamento

O fluxo completo implementado segue as seguintes etapas:

```text
1. Fonte de dados
        ↓
2. Ingestão
        ↓
3. Camada Bronze
        ↓
4. Camada Silver
        ↓
5. Validação de qualidade
        ↓
6. Camada Gold
        ↓
7. Consumo dos dados
        ↓
   Dashboard / IA
```

Esse modelo permite separar claramente as responsabilidades de cada etapa do processamento.

---

- ##  Tecnologias utilizadas

Entre as principais tecnologias e conceitos utilizados no projeto estão:

- Databricks
- Apache Spark
- PySpark
- Python
- SQL
- Delta Lake
- Databricks Jobs
- Databricks Asset Bundles
- Git
- GitHub
- APIs
- Arquitetura Medallion
- Engenharia de Dados
- Qualidade de Dados
- Inteligência Artificial
- Visualização de Dados

---

- ##  Versionamento

O projeto utiliza **Git e GitHub** para controle de versão.

O versionamento contempla tanto os códigos responsáveis pelo processamento quanto as configurações de orquestração.

O fluxo de desenvolvimento permite trabalhar com branches para implementar alterações antes de integrá-las à branch principal.

Isso proporciona maior rastreabilidade das mudanças realizadas durante a evolução do projeto.

---

- ## Evolução da solução

A arquitetura foi projetada para permitir a inclusão de novas fontes de dados.

O Mercado Pago representa uma das integrações implementadas no projeto, mas a proposta da solução é possibilitar a centralização de múltiplas fontes financeiras.

Como evolução, a plataforma poderá incorporar novas APIs e meios de pagamento mantendo o mesmo padrão arquitetural:

```text
Mercado Pago ──┐
               │
Fonte 2 ───────┼──► Plataforma de Dados ──► Gold ──► Dashboard
               │                         │
Fonte 3 ───────┘                         └──────► Agente de IA
```

Essa abordagem permite evoluir a solução sem alterar seu objetivo principal: **centralizar os dados financeiros e facilitar o acesso às informações da empresa**.

---

- ## Contexto acadêmico

Projeto desenvolvido como parte do Trabalho de Conclusão de Curso (TCC) da **Universidade de Fortaleza (UNIFOR)**.

O trabalho busca aplicar conceitos de Engenharia de Dados em um problema de negócio, contemplando desde a ingestão e transformação dos dados até sua disponibilização para análise e interação através de ferramentas de visualização e Inteligência Artificial.