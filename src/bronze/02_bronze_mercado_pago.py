from pyspark.sql import functions as F

# CONFIGURAÇÕES

CAMINHO_RAW = "/Volumes/tcc_unifor/bronze/raw_files/mercado_pago"

TABELA_BRONZE = "tcc_unifor.bronze.mercado_pago_orders"

# INÍCIO

print("=" * 70)
print("BRONZE - MERCADO PAGO")
print("=" * 70)

print(f"\nLendo arquivos JSON de:")
print(CAMINHO_RAW)


# 1. LEITURA DOS JSONs

df_raw = (
    spark.read
    .option("multiLine", True)
    .json(f"{CAMINHO_RAW}/*.json")
)

quantidade_raw = df_raw.count()

print(
    f"\nQuantidade de registros encontrados nos JSONs: "
    f"{quantidade_raw}"
)

if quantidade_raw == 0:
    raise ValueError(
        "Nenhum arquivo JSON foi encontrado no diretório RAW."
    )


# 2. NORMALIZAÇÃO DO SCHEMA

df_normalizado = df_raw.select(

    F.col("capture_mode")
        .cast("string")
        .alias("capture_mode"),

    F.col("country_code")
        .cast("string")
        .alias("country_code"),

    F.col("created_date")
        .cast("string")
        .alias("created_date"),

    F.col("currency")
        .cast("string")
        .alias("currency"),

    F.col("external_reference")
        .cast("string")
        .alias("external_reference"),

    F.col("id")
        .cast("string")
        .alias("id"),

    F.struct(
        F.col("integration_data.application_id")
            .cast("string")
            .alias("application_id")
    ).alias("integration_data"),

    F.col("last_updated_date")
        .cast("string")
        .alias("last_updated_date"),

    F.col("processing_mode")
        .cast("string")
        .alias("processing_mode"),

    F.col("status")
        .cast("string")
        .alias("status"),

    F.col("status_detail")
        .cast("string")
        .alias("status_detail"),

    F.col("total_amount")
        .cast("string")
        .alias("total_amount"),

    F.col("total_paid_amount")
        .cast("string")
        .alias("total_paid_amount"),

    F.struct(

        F.transform(
            F.col("transactions.payments"),

            lambda p: F.struct(

                p["amount"]
                    .cast("string")
                    .alias("amount"),

                p["date_of_expiration"]
                    .cast("string")
                    .alias("date_of_expiration"),

                p["id"]
                    .cast("string")
                    .alias("id"),

                F.struct(

                    p["payment_method"]["id"]
                        .cast("string")
                        .alias("id"),

                    p["payment_method"]["qr_code"]
                        .cast("string")
                        .alias("qr_code"),

                    p["payment_method"]["qr_code_base64"]
                        .cast("string")
                        .alias("qr_code_base64"),

                    p["payment_method"]["ticket_url"]
                        .cast("string")
                        .alias("ticket_url"),

                    p["payment_method"]["type"]
                        .cast("string")
                        .alias("type")

                ).alias("payment_method"),

                p["reference_id"]
                    .cast("string")
                    .alias("reference_id"),

                p["status"]
                    .cast("string")
                    .alias("status"),

                p["status_detail"]
                    .cast("string")
                    .alias("status_detail")
            )
        ).alias("payments")

    ).alias("transactions"),

    F.col("type")
        .cast("string")
        .alias("type"),

    F.col("user_id")
        .cast("string")
        .alias("user_id"),

    F.current_timestamp()
        .cast("timestamp")
        .alias("_ingestion_timestamp")
)


print("\nSchema após normalização:")

df_normalizado.printSchema()

# 3. VALIDAÇÃO DOS IDs

ids_nulos = (
    df_normalizado
    .filter(F.col("id").isNull())
    .count()
)

if ids_nulos > 0:
    raise ValueError(
        f"Foram encontrados {ids_nulos} registros sem ID."
    )


ids_distintos = (
    df_normalizado
    .select("id")
    .distinct()
    .count()
)

print(
    f"\nIDs distintos nos JSONs: {ids_distintos}"
)


# 4. VERIFICAR SE A TABELA EXISTE

tabela_existe = spark.catalog.tableExists(
    TABELA_BRONZE
)

print(
    f"\nTabela Bronze existe: {tabela_existe}"
)


# 5. PRIMEIRA CARGA

if not tabela_existe:

    print(
        "\nTabela Bronze ainda não existe."
    )

    print(
        "Criando tabela pela primeira vez..."
    )

    (
        df_normalizado
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(TABELA_BRONZE)
    )

    print(
        "\nTabela Bronze criada com sucesso."
    )


# 6. CARGA INCREMENTAL

else:

    print(
        "\nTabela Bronze já existe."
    )

    print(
        "Lendo schema existente..."
    )


    # LER TABELA E SCHEMA

    df_bronze = spark.table(
        TABELA_BRONZE
    )

    schema_tabela = df_bronze.schema


    # IDENTIFICAR IDS EXISTENTES

    ids_existentes = (
        df_bronze
        .select("id")
        .where(
            F.col("id").isNotNull()
        )
        .distinct()
    )

    quantidade_existentes = (
        ids_existentes.count()
    )

    print(
        f"IDs já existentes na Bronze: "
        f"{quantidade_existentes}"
    )


    # IDENTIFICAR NOVOS REGISTROS

    df_novos = (
        df_normalizado
        .join(
            ids_existentes,
            on="id",
            how="left_anti"
        )
    )

    quantidade_novos = (
        df_novos.count()
    )

    quantidade_ignorados = (
        quantidade_raw - quantidade_novos
    )

    print(
        f"Novos registros: {quantidade_novos}"
    )

    print(
        f"Registros já existentes/ignorados: "
        f"{quantidade_ignorados}"
    )


    # NENHUM REGISTRO NOVO

    if quantidade_novos == 0:

        print(
            "\nNenhum registro novo para inserir."
        )

    else:

        # AJUSTAR SCHEMA

        print(
            "\nAplicando o schema existente "
            "da tabela Bronze..."
        )


        df_vazio_schema = spark.createDataFrame(
            [],
            schema_tabela
        )


        df_novos_schema = (
            df_vazio_schema
            .unionByName(
                df_novos,
                allowMissingColumns=False
            )
        )

        # REMOVER O DATAFRAME VAZIO

        df_novos = df_novos_schema


        print(
            "\nSchema final dos novos registros:"
        )

        df_novos.printSchema()


        # VALIDAR QUANTIDADE

        quantidade_novos_schema = (
            df_novos.count()
        )

        print(
            f"\nQuantidade após ajuste de schema: "
            f"{quantidade_novos_schema}"
        )


        if quantidade_novos_schema != quantidade_novos:

            raise ValueError(
                "A quantidade de registros mudou "
                "durante o ajuste do schema."
            )


        # VERIFICAR CAMPOS

        campos_tabela = [
            campo.name
            for campo in schema_tabela.fields
        ]

        campos_novos = [
            campo.name
            for campo in df_novos.schema.fields
        ]


        if campos_tabela != campos_novos:

            print(
                "\nCampos da tabela:"
            )

            print(
                campos_tabela
            )

            print(
                "\nCampos dos novos dados:"
            )

            print(
                campos_novos
            )

            raise ValueError(
                "Os campos dos novos registros "
                "não correspondem aos campos da Bronze."
            )


        print(
            "\nCampos compatíveis com a Bronze."
        )


        # GRAVAÇÃO INCREMENTAL

        print(
            "\nInserindo novos registros na Bronze..."
        )


        (
            df_novos
            .write
            .format("delta")
            .mode("append")
            .saveAsTable(TABELA_BRONZE)
        )


        print(
            "\nNovos registros inseridos com sucesso."
        )


# 7. VALIDAÇÃO FINAL

print(
    "\n" + "=" * 70
)

print(
    "VALIDAÇÃO FINAL DA BRONZE"
)

print(
    "=" * 70
)


df_bronze_final = spark.table(
    TABELA_BRONZE
)


quantidade_final = (
    df_bronze_final.count()
)


ids_finais = (
    df_bronze_final
    .select("id")
    .distinct()
    .count()
)


print(
    f"\nTotal de registros na Bronze: "
    f"{quantidade_final}"
)

print(
    f"IDs distintos: {ids_finais}"
)


# 8. VALIDAR IDS NULOS

ids_nulos_final = (
    df_bronze_final
    .filter(
        F.col("id").isNull()
    )
    .count()
)


print(
    f"IDs nulos: {ids_nulos_final}"
)


if ids_nulos_final > 0:

    raise ValueError(
        "A Bronze possui registros com ID nulo."
    )


# 9. DISTRIBUIÇÃO DOS MÉTODOS DE PAGAMENTO

print(
    "\nDistribuição dos métodos de pagamento:"
)


(
    df_bronze_final

    .select(
        F.explode(
            "transactions.payments"
        ).alias("payment")
    )

    .groupBy(
        "payment.payment_method.id",
        "payment.payment_method.type"
    )

    .count()

    .orderBy(
        F.desc("count")
    )

    .show(
        truncate=False
    )
)


# 10. DISTRIBUIÇÃO DOS STATUS

print(
    "\nDistribuição dos status das ordens:"
)


(
    df_bronze_final

    .groupBy(
        "status"
    )

    .count()

    .orderBy(
        F.desc("count")
    )

    .show(
        truncate=False
    )
)


# 11. RESUMO FINANCEIRO

print(
    "\nResumo financeiro:"
)


(
    df_bronze_final

    .select(

        F.sum(
            F.col("total_amount")
            .cast("decimal(18,2)")
        ).alias(
            "valor_total"
        ),

        F.avg(
            F.col("total_amount")
            .cast("decimal(18,2)")
        ).alias(
            "ticket_medio"
        ),

        F.min(
            F.col("total_amount")
            .cast("decimal(18,2)")
        ).alias(
            "menor_valor"
        ),

        F.max(
            F.col("total_amount")
            .cast("decimal(18,2)")
        ).alias(
            "maior_valor"
        )
    )

    .show()
)


# 12. AMOSTRA FINAL

print(
    "\nAmostra dos registros mais recentes:"
)


(
    df_bronze_final

    .select(
        "id",
        "external_reference",
        "total_amount",
        "status",
        "status_detail",
        "created_date"
    )

    .orderBy(
        F.col(
            "_ingestion_timestamp"
        ).desc()
    )

    .show(
        10,
        truncate=False
    )
)


# FINAL

print(
    "\n" + "=" * 70
)

print(
    "BRONZE FINALIZADA COM SUCESSO"
)

print(
    "=" * 70
)

print(
    f"\nTabela: {TABELA_BRONZE}"
)

print(
    f"Total de registros: {quantidade_final}"
)

print(
    f"IDs distintos: {ids_finais}"
)

print(
    "\nNenhuma alteração de schema foi realizada "
    "na tabela Delta."
)

print(
    "=" * 70
)