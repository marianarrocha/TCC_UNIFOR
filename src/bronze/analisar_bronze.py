from pyspark.sql import functions as F


# ============================================================
# CONFIGURAÇÃO
# ============================================================

CAMINHO_BRONZE = (
    "/Volumes/tcc_unifor/bronze/raw_files/mercado_pago/*.json"
)

TABELA_BRONZE = (
    "tcc_unifor.bronze.mercado_pago_orders"
)


# ============================================================
# LEITURA DOS JSONS BRUTOS
# ============================================================

df = (
    spark.read
    .option("multiLine", "true")
    .json(CAMINHO_BRONZE)
)


# ============================================================
# METADADOS DE INGESTÃO
# ============================================================

df_bronze = (
    df
    .withColumn(
        "_ingestion_timestamp",
        F.current_timestamp()
    )
)


# ============================================================
# VALIDAÇÃO
# ============================================================

print("=" * 70)
print("DADOS BRONZE - MERCADO PAGO")
print("=" * 70)

print(
    f"Quantidade de registros: {df_bronze.count()}"
)

print("\nSchema:")

df_bronze.printSchema()


# ============================================================
# GRAVAÇÃO DELTA
# ============================================================

(
    df_bronze.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TABELA_BRONZE)
)


print("\n" + "=" * 70)

print(
    f"✓ Tabela criada: {TABELA_BRONZE}"
)

print("=" * 70)