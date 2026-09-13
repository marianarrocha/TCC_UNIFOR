from pyspark.sql import functions as F


# ============================================================
# CONFIGURAÇÕES
# ============================================================

TABELA_BRONZE = "tcc_unifor.bronze.mercado_pago_orders"

TABELA_SILVER = "tcc_unifor.silver.mercado_pago_payments"


# ============================================================
# INÍCIO
# ============================================================

print("=" * 70)
print("CAMADA SILVER — MERCADO PAGO")
print("=" * 70)


# ============================================================
# 1. LEITURA DA BRONZE
# ============================================================

print("\nLendo tabela Bronze...")

df_bronze = spark.table(TABELA_BRONZE)

quantidade_bronze = df_bronze.count()

print(
    f"Registros encontrados na Bronze: "
    f"{quantidade_bronze}"
)

if quantidade_bronze == 0:
    raise ValueError(
        "A tabela Bronze não possui registros."
    )


# ============================================================
# 2. EXPLOSÃO DOS PAGAMENTOS
# ============================================================

print("\n" + "=" * 70)
print("EXPLODE DOS PAGAMENTOS")
print("=" * 70)

df_silver_teste = (
    df_bronze
    .withColumn(
        "payment",
        F.explode("transactions.payments")
    )
)

quantidade_pagamentos = df_silver_teste.count()

print(
    f"\nRegistros após o explode dos pagamentos: "
    f"{quantidade_pagamentos}"
)


# ============================================================
# 3. SELEÇÃO E TRANSFORMAÇÃO DOS CAMPOS
# ============================================================

print("\n" + "=" * 70)
print("TRANSFORMAÇÃO DOS CAMPOS")
print("=" * 70)

df_silver_teste = (
    df_silver_teste
    .select(

        # ----------------------------------------------------
        # Identificação do pedido
        # ----------------------------------------------------

        F.col("id")
        .alias("order_id"),

        F.col("external_reference"),

        F.col("type")
        .alias("order_type"),

        # ----------------------------------------------------
        # Status do pedido
        # ----------------------------------------------------

        F.col("status")
        .alias("order_status"),

        F.col("status_detail")
        .alias("order_status_detail"),

        # ----------------------------------------------------
        # Dados financeiros do pedido
        # ----------------------------------------------------

        F.col("currency"),

        F.col("total_amount")
        .cast("decimal(18,2)")
        .alias("total_amount"),

        F.col("total_paid_amount")
        .cast("decimal(18,2)")
        .alias("total_paid_amount"),

        # ----------------------------------------------------
        # Datas do pedido
        # ----------------------------------------------------

        F.to_timestamp("created_date")
        .alias("created_date"),

        F.to_timestamp("last_updated_date")
        .alias("last_updated_date"),

        # ----------------------------------------------------
        # Identificação do pagamento
        # ----------------------------------------------------

        F.col("payment.id")
        .alias("payment_id"),

        # ----------------------------------------------------
        # Dados financeiros do pagamento
        # ----------------------------------------------------

        F.col("payment.amount")
        .cast("decimal(18,2)")
        .alias("payment_amount"),

        # ----------------------------------------------------
        # Status do pagamento
        # ----------------------------------------------------

        F.col("payment.status")
        .alias("payment_status"),

        F.col("payment.status_detail")
        .alias("payment_status_detail"),

        # ----------------------------------------------------
        # Referência do pagamento
        # ----------------------------------------------------

        F.col("payment.reference_id")
        .alias("payment_reference_id"),

        # ----------------------------------------------------
        # Método de pagamento
        # ----------------------------------------------------

        F.col("payment.payment_method.id")
        .alias("payment_method_id"),

        F.col("payment.payment_method.type")
        .alias("payment_method_type"),

        # ----------------------------------------------------
        # Expiração
        # ----------------------------------------------------

        F.col("payment.date_of_expiration")
        .alias("payment_expiration"),

        # ----------------------------------------------------
        # Controle de ingestão
        # ----------------------------------------------------

        F.col("_ingestion_timestamp")
    )
)


# ============================================================
# 4. SCHEMA RESULTANTE
# ============================================================

print("\n" + "=" * 70)
print("SCHEMA DA SILVER")
print("=" * 70)

df_silver_teste.printSchema()


# ============================================================
# 5. VALIDAÇÃO DOS REGISTROS
# ============================================================

print("\n" + "=" * 70)
print("VALIDAÇÃO DOS REGISTROS")
print("=" * 70)

print(
    f"\nRegistros na Bronze: "
    f"{quantidade_bronze}"
)

print(
    f"Registros na Silver após explode: "
    f"{quantidade_pagamentos}"
)


# ============================================================
# 6. VALIDAÇÃO DE IDS
# ============================================================

print("\n" + "=" * 70)
print("VALIDAÇÃO DOS IDENTIFICADORES")
print("=" * 70)

ids_pedido_nulos = (
    df_silver_teste
    .filter(F.col("order_id").isNull())
    .count()
)

ids_pagamento_nulos = (
    df_silver_teste
    .filter(F.col("payment_id").isNull())
    .count()
)

print(
    f"\nOrder IDs nulos: "
    f"{ids_pedido_nulos}"
)

print(
    f"Payment IDs nulos: "
    f"{ids_pagamento_nulos}"
)


# ============================================================
# 7. VISUALIZAÇÃO DOS DADOS
# ============================================================

print("\n" + "=" * 70)
print("AMOSTRA DOS DADOS SILVER")
print("=" * 70)

(
    df_silver_teste
    .select(
        "order_id",
        "external_reference",
        "total_amount",
        "payment_id",
        "payment_amount",
        "payment_method_id",
        "payment_method_type",
        "payment_status",
        "payment_status_detail",
        "created_date"
    )
    .show(
        10,
        truncate=False
    )
)


# ============================================================
# 8. RESUMO FINANCEIRO
# ============================================================

print("\n" + "=" * 70)
print("RESUMO FINANCEIRO")
print("=" * 70)

(
    df_silver_teste
    .select(
        F.count("*")
        .alias("quantidade_pagamentos"),

        F.sum("payment_amount")
        .alias("valor_total_pagamentos"),

        F.avg("payment_amount")
        .alias("valor_medio_pagamento"),

        F.min("payment_amount")
        .alias("menor_pagamento"),

        F.max("payment_amount")
        .alias("maior_pagamento")
    )
    .show(
        truncate=False
    )
)


# ============================================================
# 9. DISTRIBUIÇÃO DOS MÉTODOS DE PAGAMENTO
# ============================================================

print("\n" + "=" * 70)
print("MÉTODOS DE PAGAMENTO")
print("=" * 70)

(
    df_silver_teste
    .groupBy(
        "payment_method_id",
        "payment_method_type"
    )
    .count()
    .orderBy(F.desc("count"))
    .show(
        truncate=False
    )
)

# ============================================================
# 10. VALIDAÇÃO DE DUPLICIDADE DOS PAGAMENTOS
# ============================================================

print("\n" + "=" * 70)
print("VALIDAÇÃO DE DUPLICIDADE")
print("=" * 70)

quantidade_pagamentos_total = (
    df_silver_teste.count()
)

quantidade_payment_ids = (
    df_silver_teste
    .select("payment_id")
    .distinct()
    .count()
)

duplicidades_pagamento = (
    quantidade_pagamentos_total
    - quantidade_payment_ids
)

print(
    f"\nTotal de pagamentos: "
    f"{quantidade_pagamentos_total}"
)

print(
    f"Payment IDs distintos: "
    f"{quantidade_payment_ids}"
)

print(
    f"Duplicidades: "
    f"{duplicidades_pagamento}"
)

if duplicidades_pagamento > 0:
    raise ValueError(
        "Foram encontrados pagamentos duplicados "
        "na transformação Silver."
    )

print(
    "\n✓ Nenhuma duplicidade encontrada."
)

# ============================================================
# 11. GRAVAÇÃO DA SILVER
# ============================================================

print("\n" + "=" * 70)
print("GRAVAÇÃO DA TABELA SILVER")
print("=" * 70)

(
    df_silver_teste
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(TABELA_SILVER)
)

print(
    f"\n✓ Tabela Silver criada: "
    f"{TABELA_SILVER}"
)

# ============================================================
# 12. VALIDAÇÃO DA TABELA SILVER
# ============================================================

print("\n" + "=" * 70)
print("VALIDAÇÃO FINAL DA SILVER")
print("=" * 70)

df_silver = spark.table(TABELA_SILVER)

quantidade_silver = df_silver.count()

print(
    f"\nRegistros gravados na Silver: "
    f"{quantidade_silver}"
)

if quantidade_silver != quantidade_pagamentos_total:
    raise ValueError(
        "A quantidade de registros gravados "
        "na Silver não corresponde à quantidade "
        "esperada."
    )

print(
    "\n✓ Quantidade de registros validada."
)

print(
    f"✓ Tabela: {TABELA_SILVER}"
)

print(
    "✓ Silver gravada com sucesso."
)

print("\n" + "=" * 70)
print("SILVER CONCLUÍDA COM SUCESSO")
print("=" * 70)