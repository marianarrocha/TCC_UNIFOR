# ============================================================
# 05_gold_mercado_pago.py
# Camada Gold - Mercado Pago
# ============================================================

from pyspark.sql import functions as F


# ============================================================
# 1. CONFIGURAÇÕES
# ============================================================

TABELA_SILVER = "tcc_unifor.silver.mercado_pago_payments"
TABELA_GOLD = "tcc_unifor.gold.mercado_pago_resumo"


# ============================================================
# 2. LEITURA DA SILVER
# ============================================================

print("=" * 70)
print("INICIANDO CAMADA GOLD - MERCADO PAGO")
print("=" * 70)

print(f"\nLendo tabela Silver: {TABELA_SILVER}")

df_silver = spark.table(TABELA_SILVER)

quantidade_silver = df_silver.count()

print(f"Registros encontrados na Silver: {quantidade_silver}")

if quantidade_silver == 0:
    raise ValueError("A tabela Silver está vazia. Gold não pode ser criada.")


# ============================================================
# 3. PREPARAÇÃO DOS DADOS
# ============================================================

print("\nPreparando dados para agregação...")

df_base = (
    df_silver
    .withColumn(
        "data_referencia",
        F.to_date("created_date")
    )
    .withColumn(
        "valor_pagamento",
        F.col("payment_amount").cast("decimal(18,2)")
    )
)


# ============================================================
# 4. AGREGAÇÃO GOLD
# ============================================================

print("Calculando indicadores analíticos...")


df_gold = (
    df_base
    .groupBy(
        "data_referencia",
        "payment_status",
        "payment_method_id",
        "payment_method_type"
    )
    .agg(
        # Quantidade de pagamentos
        F.countDistinct("payment_id")
        .alias("quantidade_pagamentos"),

        # Valor total dos pagamentos
        F.sum("valor_pagamento")
        .cast("decimal(18,2)")
        .alias("valor_total"),

        # Ticket médio
        F.avg("valor_pagamento")
        .cast("decimal(18,2)")
        .alias("ticket_medio"),

        # Menor pagamento
        F.min("valor_pagamento")
        .cast("decimal(18,2)")
        .alias("menor_pagamento"),

        # Maior pagamento
        F.max("valor_pagamento")
        .cast("decimal(18,2)")
        .alias("maior_pagamento")
    )
)


# ============================================================
# 5. INDICADORES PERCENTUAIS
# ============================================================

print("Calculando participação percentual...")


# Total de pagamentos por data
window_data = (
    __import__("pyspark.sql.window", fromlist=["Window"])
    .Window
    .partitionBy("data_referencia")
)


df_gold = (
    df_gold
    .withColumn(
        "total_pagamentos_dia",
        F.sum("quantidade_pagamentos").over(window_data)
    )
    .withColumn(
        "percentual_pagamentos",
        F.round(
            (
                F.col("quantidade_pagamentos")
                / F.col("total_pagamentos_dia")
            ) * 100,
            2
        )
    )
    .drop("total_pagamentos_dia")
)


# ============================================================
# 6. ORDENAÇÃO
# ============================================================

df_gold = df_gold.orderBy(
    "data_referencia",
    "payment_status",
    "payment_method_id"
)


# ============================================================
# 7. VALIDAÇÕES
# ============================================================

print("\n" + "=" * 70)
print("VALIDAÇÕES DA GOLD")
print("=" * 70)

quantidade_gold = df_gold.count()

print(f"\nQuantidade de registros Gold: {quantidade_gold}")

if quantidade_gold == 0:
    raise ValueError("A tabela Gold ficou vazia após as agregações.")


# Verificação de valores nulos nos principais indicadores
colunas_indicadores = [
    "quantidade_pagamentos",
    "valor_total",
    "ticket_medio",
    "menor_pagamento",
    "maior_pagamento"
]

print("\nValores nulos nos indicadores:")

for coluna in colunas_indicadores:

    quantidade_nulos = (
        df_gold
        .filter(F.col(coluna).isNull())
        .count()
    )

    print(f"  {coluna}: {quantidade_nulos}")

    if quantidade_nulos > 0:
        raise ValueError(
            f"O indicador '{coluna}' possui valores nulos."
        )


# Verificação de valores positivos
valores_invalidos = (
    df_gold
    .filter(
        (F.col("quantidade_pagamentos") <= 0)
        | (F.col("valor_total") <= 0)
        | (F.col("ticket_medio") <= 0)
    )
    .count()
)

print(f"\nRegistros com indicadores inválidos: {valores_invalidos}")

if valores_invalidos > 0:
    raise ValueError(
        "Foram encontrados indicadores Gold inválidos."
    )


# Verificação do percentual
percentuais_invalidos = (
    df_gold
    .filter(
        (F.col("percentual_pagamentos") < 0)
        | (F.col("percentual_pagamentos") > 100)
    )
    .count()
)

print(
    f"Registros com percentual inválido: "
    f"{percentuais_invalidos}"
)

if percentuais_invalidos > 0:
    raise ValueError(
        "Foram encontrados percentuais inválidos na Gold."
    )


# ============================================================
# 8. RESUMO DOS INDICADORES
# ============================================================

print("\n" + "=" * 70)
print("RESUMO DOS INDICADORES GOLD")
print("=" * 70)

df_gold.select(
    F.sum("quantidade_pagamentos")
        .alias("total_pagamentos"),

    F.sum("valor_total")
        .cast("decimal(18,2)")
        .alias("valor_total"),

    F.avg("ticket_medio")
        .cast("decimal(18,2)")
        .alias("ticket_medio_medio"),

    F.min("menor_pagamento")
        .cast("decimal(18,2)")
        .alias("menor_pagamento"),

    F.max("maior_pagamento")
        .cast("decimal(18,2)")
        .alias("maior_pagamento")
).show(truncate=False)


# ============================================================
# 9. DISTRIBUIÇÃO POR STATUS
# ============================================================

print("\nDistribuição por status:")

df_gold.groupBy(
    "payment_status"
).agg(
    F.sum("quantidade_pagamentos")
        .alias("quantidade_pagamentos"),

    F.sum("valor_total")
        .cast("decimal(18,2)")
        .alias("valor_total")
).orderBy(
    F.desc("quantidade_pagamentos")
).show(truncate=False)


# ============================================================
# 10. DISTRIBUIÇÃO POR MÉTODO DE PAGAMENTO
# ============================================================

print("\nDistribuição por método de pagamento:")

df_gold.groupBy(
    "payment_method_id",
    "payment_method_type"
).agg(
    F.sum("quantidade_pagamentos")
        .alias("quantidade_pagamentos"),

    F.sum("valor_total")
        .cast("decimal(18,2)")
        .alias("valor_total")
).orderBy(
    F.desc("quantidade_pagamentos")
).show(truncate=False)


# ============================================================
# 11. AMOSTRA DA GOLD
# ============================================================

print("\nAmostra da tabela Gold:")

df_gold.show(
    20,
    truncate=False
)


# ============================================================
# 12. CRIAÇÃO DO SCHEMA GOLD
# ============================================================

print("\nCriando schema Gold, caso não exista...")

spark.sql("""
    CREATE SCHEMA IF NOT EXISTS tcc_unifor.gold
""")


# ============================================================
# 13. GRAVAÇÃO DA TABELA GOLD
# ============================================================

print(f"\nGravando tabela: {TABELA_GOLD}")

(
    df_gold
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TABELA_GOLD)
)


# ============================================================
# 14. VALIDAÇÃO FINAL
# ============================================================

print("\nValidando tabela Gold criada...")

df_gold_final = spark.table(TABELA_GOLD)

quantidade_final = df_gold_final.count()

print(f"Registros gravados na Gold: {quantidade_final}")

if quantidade_final != quantidade_gold:
    raise ValueError(
        "A quantidade de registros após a gravação "
        "não corresponde à quantidade calculada."
    )


print("\n" + "=" * 70)
print("GOLD MERCADO PAGO CRIADA COM SUCESSO")
print("=" * 70)

print(f"\nTabela: {TABELA_GOLD}")
print(f"Registros: {quantidade_final}")

print("\nIndicadores disponíveis:")
print("  - Quantidade de pagamentos")
print("  - Valor total")
print("  - Ticket médio")
print("  - Menor pagamento")
print("  - Maior pagamento")
print("  - Percentual de pagamentos")
print("  - Distribuição por status")
print("  - Distribuição por método de pagamento")
print("  - Data de referência")

print("\nPipeline Bronze -> Silver -> Gold concluído.")
