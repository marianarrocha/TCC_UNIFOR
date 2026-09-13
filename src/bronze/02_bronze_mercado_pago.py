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
# INÍCIO
# ============================================================

print("=" * 70)
print("CAMADA BRONZE — MERCADO PAGO")
print("=" * 70)

print("\nLendo arquivos JSON brutos...")


# ============================================================
# LEITURA DOS JSONS
# ============================================================

df = (
    spark.read
    .option("multiLine", "true")
    .json(CAMINHO_BRONZE)
)


# ============================================================
# QUANTIDADE DE REGISTROS LIDOS
# ============================================================

quantidade_lidos = df.count()

print(
    f"\nQuantidade de registros encontrados: "
    f"{quantidade_lidos}"
)

if quantidade_lidos == 0:

    raise ValueError(
        "Nenhum arquivo JSON foi encontrado "
        "no caminho da Bronze."
    )


# ============================================================
# SCHEMA
# ============================================================

print("\n" + "=" * 70)
print("SCHEMA DOS DADOS")
print("=" * 70)

df.printSchema()


# ============================================================
# VALIDAÇÃO DE ID NULO
# ============================================================

print("\n" + "=" * 70)
print("VALIDAÇÃO DE IDENTIFICADORES")
print("=" * 70)

ids_nulos = (
    df
    .filter(F.col("id").isNull())
    .count()
)

print(
    f"\nIDs nulos: {ids_nulos}"
)

if ids_nulos > 0:

    raise ValueError(
        "Foram encontrados registros sem ID."
    )


# ============================================================
# VALIDAÇÃO DE DUPLICIDADE NOS ARQUIVOS
# ============================================================

quantidade_ids = (
    df
    .select("id")
    .distinct()
    .count()
)

duplicidades = (
    quantidade_lidos - quantidade_ids
)

print(
    f"IDs distintos: {quantidade_ids}"
)

print(
    f"Registros duplicados nos arquivos: "
    f"{duplicidades}"
)

if duplicidades > 0:

    raise ValueError(
        "Foram encontrados IDs duplicados "
        "nos arquivos JSON."
    )


# ============================================================
# VALIDAÇÃO DOS CAMPOS PRINCIPAIS
# ============================================================

print("\n" + "=" * 70)
print("VALIDAÇÃO DE CAMPOS PRINCIPAIS")
print("=" * 70)

campos_principais = [
    "id",
    "type",
    "external_reference",
    "total_amount",
    "currency",
    "status",
    "created_date"
]

for campo in campos_principais:

    quantidade_nulos = (
        df
        .filter(F.col(campo).isNull())
        .count()
    )

    print(
        f"{campo}: "
        f"{quantidade_nulos} valores nulos"
    )


# ============================================================
# RESUMO FINANCEIRO DOS ARQUIVOS LIDOS
# ============================================================

print("\n" + "=" * 70)
print("RESUMO DOS VALORES")
print("=" * 70)

resumo = (
    df
    .select(
        F.count("*").alias("quantidade"),
        F.min(
            F.col("total_amount")
            .cast("decimal(18,2)")
        ).alias("valor_minimo"),
        F.max(
            F.col("total_amount")
            .cast("decimal(18,2)")
        ).alias("valor_maximo"),
        F.sum(
            F.col("total_amount")
            .cast("decimal(18,2)")
        ).alias("valor_total"),
        F.avg(
            F.col("total_amount")
            .cast("decimal(18,2)")
        ).alias("valor_medio")
    )
)

resumo.show(truncate=False)


# ============================================================
# DISTRIBUIÇÃO DOS STATUS
# ============================================================

print("\n" + "=" * 70)
print("DISTRIBUIÇÃO DOS STATUS")
print("=" * 70)

(
    df
    .groupBy("status")
    .count()
    .orderBy(F.desc("count"))
    .show(truncate=False)
)


# ============================================================
# AMOSTRA DOS DADOS
# ============================================================

print("\n" + "=" * 70)
print("AMOSTRA DOS DADOS")
print("=" * 70)

(
    df
    .select(
        "id",
        "external_reference",
        "total_amount",
        "currency",
        "status",
        "created_date"
    )
    .show(10, truncate=False)
)


# ============================================================
# VERIFICAÇÃO DA TABELA BRONZE
# ============================================================

print("\n" + "=" * 70)
print("VERIFICAÇÃO DA TABELA BRONZE")
print("=" * 70)


tabela_existe = (
    spark.catalog.tableExists(TABELA_BRONZE)
)


# ============================================================
# PRIMEIRA CARGA
# ============================================================

if not tabela_existe:

    print(
        "\nTabela Bronze ainda não existe."
    )

    print(
        "Será realizada a primeira carga."
    )

    quantidade_antes = 0

    df_novos = df


# ============================================================
# CARGAS SEGUINTES
# ============================================================

else:

    print(
        "\nTabela Bronze já existe."
    )

    # --------------------------------------------------------
    # QUANTIDADE ATUAL DA TABELA
    # --------------------------------------------------------

    quantidade_antes = (
        spark
        .table(TABELA_BRONZE)
        .count()
    )

    print(
        f"Registros existentes antes da carga: "
        f"{quantidade_antes}"
    )

    # --------------------------------------------------------
    # LEITURA DOS IDs JÁ EXISTENTES
    # --------------------------------------------------------

    df_ids_existentes = (
        spark
        .table(TABELA_BRONZE)
        .select("id")
        .distinct()
    )

    quantidade_ids_existentes = (
        df_ids_existentes.count()
    )

    print(
        f"IDs já existentes na Bronze: "
        f"{quantidade_ids_existentes}"
    )

    # --------------------------------------------------------
    # IDENTIFICAÇÃO DE NOVOS REGISTROS
    # --------------------------------------------------------

    df_novos = (
        df
        .join(
            df_ids_existentes,
            on="id",
            how="left_anti"
        )
    )


# ============================================================
# QUANTIDADE DE NOVOS REGISTROS
# ============================================================

quantidade_novos = df_novos.count()

print(
    f"\nNovos registros identificados: "
    f"{quantidade_novos}"
)


# ============================================================
# VERIFICAÇÃO DE DUPLICIDADES ENTRE CARGAS
# ============================================================

quantidade_ignorados = (
    quantidade_lidos - quantidade_novos
)

print(
    f"Registros já existentes/ignorados: "
    f"{quantidade_ignorados}"
)


# ============================================================
# ADIÇÃO DO TIMESTAMP DE INGESTÃO
# ============================================================

if quantidade_novos > 0:

    df_bronze = (
        df_novos
        .withColumn(
            "_ingestion_timestamp",
            F.current_timestamp()
        )
    )


    # ========================================================
    # GRAVAÇÃO INCREMENTAL
    # ========================================================

    print("\n" + "=" * 70)
    print("GRAVAÇÃO INCREMENTAL DA BRONZE")
    print("=" * 70)

    (
        df_bronze.write
        .format("delta")
        .mode("append")
        .saveAsTable(TABELA_BRONZE)
    )

    print(
        f"\n✓ {quantidade_novos} novos registros "
        "adicionados à Bronze."
    )

else:

    print("\n" + "=" * 70)
    print("NENHUM NOVO REGISTRO")
    print("=" * 70)

    print(
        "\n✓ Nenhum registro novo foi encontrado."
    )

    print(
        "✓ A tabela Bronze não foi alterada."
    )


# ============================================================
# VALIDAÇÃO FINAL
# ============================================================

quantidade_depois = (
    spark
    .table(TABELA_BRONZE)
    .count()
)

quantidade_esperada = (
    quantidade_antes + quantidade_novos
)


print("\n" + "=" * 70)
print("VALIDAÇÃO FINAL DA CARGA")
print("=" * 70)

print(
    f"\nRegistros antes da carga: "
    f"{quantidade_antes}"
)

print(
    f"Registros lidos dos JSONs: "
    f"{quantidade_lidos}"
)

print(
    f"Registros novos: "
    f"{quantidade_novos}"
)

print(
    f"Registros ignorados: "
    f"{quantidade_ignorados}"
)

print(
    f"Registros depois da carga: "
    f"{quantidade_depois}"
)

print(
    f"Quantidade esperada: "
    f"{quantidade_esperada}"
)


if quantidade_depois != quantidade_esperada:

    raise ValueError(
        "A quantidade de registros na tabela Bronze "
        "não corresponde à quantidade esperada."
    )


# ============================================================
# VALIDAÇÃO DE DUPLICIDADE NA TABELA FINAL
# ============================================================

print("\n" + "=" * 70)
print("VALIDAÇÃO DA TABELA BRONZE")
print("=" * 70)

quantidade_total = (
    spark
    .table(TABELA_BRONZE)
    .count()
)

quantidade_ids_total = (
    spark
    .table(TABELA_BRONZE)
    .select("id")
    .distinct()
    .count()
)

duplicidades_tabela = (
    quantidade_total - quantidade_ids_total
)

print(
    f"\nTotal de registros: "
    f"{quantidade_total}"
)

print(
    f"IDs distintos: "
    f"{quantidade_ids_total}"
)

print(
    f"Duplicidades na tabela: "
    f"{duplicidades_tabela}"
)


if duplicidades_tabela > 0:

    raise ValueError(
        "Foram encontradas duplicidades "
        "na tabela Bronze."
    )


# ============================================================
# CONCLUSÃO
# ============================================================

print("\n" + "=" * 70)
print("BRONZE CONCLUÍDA COM SUCESSO")
print("=" * 70)

print(
    f"\n✓ Tabela Delta: "
    f"{TABELA_BRONZE}"
)

print(
    f"✓ Registros lidos: "
    f"{quantidade_lidos}"
)

print(
    f"✓ Novos registros adicionados: "
    f"{quantidade_novos}"
)

print(
    f"✓ Registros ignorados: "
    f"{quantidade_ignorados}"
)

print(
    f"✓ Total acumulado na Bronze: "
    f"{quantidade_depois}"
)

print(
    "✓ Validações executadas"
)

print(
    "✓ Timestamp de ingestão adicionado"
)

print(
    "✓ Modo de carga: APPEND incremental"
)

print("\n" + "=" * 70)
