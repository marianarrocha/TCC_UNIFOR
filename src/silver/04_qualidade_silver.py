from pyspark.sql import functions as F


# ============================================================
# CONFIGURAÇÕES
# ============================================================

TABELA_SILVER = "tcc_unifor.silver.mercado_pago_payments"


# ============================================================
# INÍCIO
# ============================================================

print("=" * 70)
print("DATA QUALITY — SILVER MERCADO PAGO")
print("=" * 70)


# ============================================================
# 1. LEITURA DA SILVER
# ============================================================

print("\nLendo tabela Silver...")

df = spark.table(TABELA_SILVER)

quantidade_total = df.count()

print(
    f"Registros encontrados: {quantidade_total}"
)

if quantidade_total == 0:
    raise ValueError(
        "A tabela Silver está vazia."
    )


# ============================================================
# 2. VERIFICAÇÃO DE CAMPOS OBRIGATÓRIOS
# ============================================================

print("\n" + "=" * 70)
print("1. CAMPOS OBRIGATÓRIOS")
print("=" * 70)

campos_obrigatorios = [
    "order_id",
    "payment_id",
    "external_reference",
    "currency",
    "payment_amount",
    "payment_status",
    "payment_method_id",
    "payment_method_type",
    "created_date"
]

erros_nulos = 0

for campo in campos_obrigatorios:

    quantidade_nulos = (
        df
        .filter(F.col(campo).isNull())
        .count()
    )

    print(
        f"{campo}: "
        f"{quantidade_nulos} nulos"
    )

    erros_nulos += quantidade_nulos


if erros_nulos > 0:
    raise ValueError(
        "Foram encontrados valores nulos "
        "em campos obrigatórios."
    )

print(
    "\n✓ Todos os campos obrigatórios "
    "estão preenchidos."
)


# ============================================================
# 3. DUPLICIDADE DE PAYMENT_ID
# ============================================================

print("\n" + "=" * 70)
print("2. DUPLICIDADE DE PAGAMENTOS")
print("=" * 70)

quantidade_payment_ids = (
    df
    .select("payment_id")
    .distinct()
    .count()
)

duplicidades = (
    quantidade_total
    - quantidade_payment_ids
)

print(
    f"\nTotal de registros: "
    f"{quantidade_total}"
)

print(
    f"Payment IDs distintos: "
    f"{quantidade_payment_ids}"
)

print(
    f"Duplicidades encontradas: "
    f"{duplicidades}"
)

if duplicidades > 0:
    raise ValueError(
        "Foram encontrados payment_ids duplicados."
    )

print(
    "\n✓ Nenhum pagamento duplicado."
)


# ============================================================
# 4. VALORES DOS PAGAMENTOS
# ============================================================

print("\n" + "=" * 70)
print("3. VALIDAÇÃO DOS VALORES")
print("=" * 70)

valores_invalidos = (
    df
    .filter(
        (F.col("payment_amount").isNull())
        | (F.col("payment_amount") <= 0)
    )
    .count()
)

print(
    f"\nPagamentos com valor inválido: "
    f"{valores_invalidos}"
)

if valores_invalidos > 0:
    raise ValueError(
        "Foram encontrados pagamentos "
        "com valores menores ou iguais a zero."
    )

print(
    "\n✓ Todos os pagamentos possuem "
    "valor maior que zero."
)


# ============================================================
# 5. CONSISTÊNCIA ENTRE TOTAL DO PEDIDO E PAGAMENTO
# ============================================================

print("\n" + "=" * 70)
print("4. CONSISTÊNCIA DOS VALORES")
print("=" * 70)

inconsistencias_valor = (
    df
    .filter(
        F.col("total_amount").isNotNull()
        & F.col("payment_amount").isNotNull()
        & (
            F.col("total_amount")
            != F.col("payment_amount")
        )
    )
    .count()
)

print(
    f"\nInconsistências encontradas: "
    f"{inconsistencias_valor}"
)

if inconsistencias_valor > 0:
    raise ValueError(
        "Foram encontradas diferenças entre "
        "total_amount e payment_amount."
    )

print(
    "\n✓ Valores do pedido e pagamento "
    "estão consistentes."
)


# ============================================================
# 6. VALIDAÇÃO DA MOEDA
# ============================================================

print("\n" + "=" * 70)
print("5. VALIDAÇÃO DA MOEDA")
print("=" * 70)

moedas = (
    df
    .groupBy("currency")
    .count()
    .orderBy(F.desc("count"))
)

moedas.show(truncate=False)

quantidade_moedas_invalidas = (
    df
    .filter(
        F.col("currency").isNull()
        | (F.col("currency") != "BRL")
    )
    .count()
)

print(
    f"\nRegistros com moeda diferente de BRL: "
    f"{quantidade_moedas_invalidas}"
)

if quantidade_moedas_invalidas > 0:
    raise ValueError(
        "Foram encontrados registros "
        "com moeda diferente de BRL."
    )

print(
    "\n✓ Todos os registros estão em BRL."
)


# ============================================================
# 7. VALIDAÇÃO DO MÉTODO DE PAGAMENTO
# ============================================================

print("\n" + "=" * 70)
print("6. MÉTODOS DE PAGAMENTO")
print("=" * 70)

(
    df
    .groupBy(
        "payment_method_id",
        "payment_method_type"
    )
    .count()
    .orderBy(F.desc("count"))
    .show(truncate=False)
)

metodos_nulos = (
    df
    .filter(
        F.col("payment_method_id").isNull()
        | F.col("payment_method_type").isNull()
    )
    .count()
)

print(
    f"\nMétodos de pagamento incompletos: "
    f"{metodos_nulos}"
)

if metodos_nulos > 0:
    raise ValueError(
        "Foram encontrados métodos "
        "de pagamento incompletos."
    )

print(
    "\n✓ Métodos de pagamento preenchidos."
)


# ============================================================
# 8. VALIDAÇÃO DAS DATAS
# ============================================================

print("\n" + "=" * 70)
print("7. VALIDAÇÃO DAS DATAS")
print("=" * 70)

datas_invalidas = (
    df
    .filter(
        F.col("created_date").isNull()
        | F.col("last_updated_date").isNull()
    )
    .count()
)

print(
    f"\nRegistros com datas inválidas: "
    f"{datas_invalidas}"
)

if datas_invalidas > 0:
    raise ValueError(
        "Foram encontrados registros "
        "com datas inválidas."
    )

print(
    "\n✓ Datas preenchidas corretamente."
)


# ============================================================
# 9. VALIDAÇÃO TEMPORAL
# ============================================================

print("\n" + "=" * 70)
print("8. CONSISTÊNCIA TEMPORAL")
print("=" * 70)

datas_inconsistentes = (
    df
    .filter(
        F.col("last_updated_date")
        < F.col("created_date")
    )
    .count()
)

print(
    f"\nRegistros com last_updated_date "
    f"anterior à created_date: "
    f"{datas_inconsistentes}"
)

if datas_inconsistentes > 0:
    raise ValueError(
        "Foram encontradas inconsistências "
        "nas datas dos pagamentos."
    )

print(
    "\n✓ Consistência temporal validada."
)


# ============================================================
# 10. VALIDAÇÃO DO TIMESTAMP DE INGESTÃO
# ============================================================

print("\n" + "=" * 70)
print("9. TIMESTAMP DE INGESTÃO")
print("=" * 70)

ingestion_nulos = (
    df
    .filter(
        F.col("_ingestion_timestamp").isNull()
    )
    .count()
)

print(
    f"\nTimestamps de ingestão nulos: "
    f"{ingestion_nulos}"
)

if ingestion_nulos > 0:
    raise ValueError(
        "Foram encontrados registros "
        "sem timestamp de ingestão."
    )

print(
    "\n✓ Timestamp de ingestão presente "
    "em todos os registros."
)


# ============================================================
# 11. RESUMO DA QUALIDADE
# ============================================================

print("\n" + "=" * 70)
print("RESUMO DA DATA QUALITY")
print("=" * 70)

print(
    f"\nTotal de registros avaliados: "
    f"{quantidade_total}"
)

print(
    "✓ Campos obrigatórios preenchidos"
)

print(
    "✓ Nenhuma duplicidade de payment_id"
)

print(
    "✓ Valores financeiros válidos"
)

print(
    "✓ Valores do pedido e pagamento consistentes"
)

print(
    "✓ Moeda validada"
)

print(
    "✓ Métodos de pagamento validados"
)

print(
    "✓ Datas validadas"
)

print(
    "✓ Consistência temporal validada"
)

print(
    "✓ Timestamp de ingestão validado"
)


# ============================================================
# FINAL
# ============================================================

print("\n" + "=" * 70)
print("DATA QUALITY CONCLUÍDA COM SUCESSO")
print("=" * 70)

print(
    "\n✓ Silver aprovada em todas as validações."
)

print(
    "✓ Nenhuma inconsistência crítica encontrada."
)

print(
    "✓ Dados prontos para a próxima etapa: GOLD."
)

print("\n" + "=" * 70)