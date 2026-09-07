import json
from pathlib import Path

import pandas as pd


# ============================================================
# CONFIGURAÇÃO
# ============================================================

# Diretório raiz do projeto
RAIZ_PROJETO = Path(__file__).resolve().parents[2]

# Diretório contendo os dados brutos da Bronze
PASTA_BRONZE = (
    RAIZ_PROJETO
    / "data"
    / "bronze"
    / "mercado_pago"
)

# Relatório auxiliar de qualidade
ARQUIVO_RELATORIO = (
    RAIZ_PROJETO
    / "data"
    / "bronze"
    / "resumo_transacoes.csv"
)


# ============================================================
# INÍCIO
# ============================================================

print("=" * 70)
print("ANÁLISE E QUALIDADE DA CAMADA BRONZE")
print("=" * 70)

print(f"\nOrigem dos dados:")
print(f"  {PASTA_BRONZE}")


# ============================================================
# VALIDAÇÃO DO DIRETÓRIO
# ============================================================

if not PASTA_BRONZE.exists():

    raise FileNotFoundError(
        "Diretório da camada Bronze não encontrado:\n"
        f"{PASTA_BRONZE}"
    )

if not PASTA_BRONZE.is_dir():

    raise NotADirectoryError(
        "O caminho informado não é um diretório:\n"
        f"{PASTA_BRONZE}"
    )


# ============================================================
# LOCALIZAÇÃO DOS JSONs
# ============================================================

arquivos = sorted(
    PASTA_BRONZE.glob("*.json")
)

print("\n" + "=" * 70)
print("1. ARQUIVOS ENCONTRADOS")
print("=" * 70)

print(
    f"\nQuantidade de arquivos JSON: {len(arquivos)}"
)

if not arquivos:

    raise FileNotFoundError(
        "Nenhum arquivo JSON encontrado na camada Bronze."
    )


# ============================================================
# LEITURA DOS JSONs
# ============================================================

registros = []
erros_leitura = []

print("\n" + "=" * 70)
print("2. LEITURA DOS ARQUIVOS")
print("=" * 70)

for arquivo in arquivos:

    try:

        with open(
            arquivo,
            "r",
            encoding="utf-8"
        ) as f:

            dados = json.load(f)

        registros.append(
            {
                "arquivo": arquivo.name,
                "dados": dados
            }
        )

        print(
            f"✓ {arquivo.name}"
        )

    except Exception as erro:

        erros_leitura.append(
            {
                "arquivo": arquivo.name,
                "erro": str(erro)
            }
        )

        print(
            f"✗ {arquivo.name}"
        )

        print(
            f"  Erro: {erro}"
        )


print(
    f"\nArquivos lidos com sucesso: "
    f"{len(registros)}"
)

print(
    f"Arquivos com erro de leitura: "
    f"{len(erros_leitura)}"
)


# ============================================================
# VALIDAÇÃO DE SCHEMA
# ============================================================

print("\n" + "=" * 70)
print("3. VALIDAÇÃO DO SCHEMA")
print("=" * 70)

schemas = {}

for registro in registros:

    dados = registro["dados"]

    if isinstance(dados, dict):

        schema = tuple(
            sorted(dados.keys())
        )

        schemas.setdefault(
            schema,
            []
        ).append(
            registro["arquivo"]
        )

    else:

        schema = (
            "JSON não possui objeto "
            "no nível principal"
        )

        schemas.setdefault(
            schema,
            []
        ).append(
            registro["arquivo"]
        )


print(
    f"\nSchemas diferentes encontrados: "
    f"{len(schemas)}"
)

for numero, (schema, arquivos_schema) in enumerate(
    schemas.items(),
    start=1
):

    print(
        f"\nSchema {numero}: "
        f"{len(arquivos_schema)} arquivo(s)"
    )

    if isinstance(schema, tuple):

        print(
            "Campos:"
        )

        for campo in schema:

            print(
                f"  - {campo}"
            )

    else:

        print(
            f"  {schema}"
        )


# ============================================================
# EXTRAÇÃO DOS PAGAMENTOS
# ============================================================

print("\n" + "=" * 70)
print("4. EXTRAÇÃO DOS REGISTROS DE PAGAMENTO")
print("=" * 70)

pagamentos = []

for registro in registros:

    arquivo = registro["arquivo"]
    dados = registro["dados"]

    if not isinstance(dados, dict):

        continue

    transactions = dados.get(
        "transactions",
        {}
    )

    if not isinstance(transactions, dict):

        continue

    payments = transactions.get(
        "payments",
        []
    )

    if not isinstance(payments, list):

        continue

    for pagamento in payments:

        if not isinstance(pagamento, dict):

            continue

        pagamentos.append(
            {
                "arquivo": arquivo,
                "order_id": dados.get("id"),
                "external_reference": dados.get(
                    "external_reference"
                ),
                "order_status": dados.get(
                    "status"
                ),
                "payment_id": pagamento.get(
                    "id"
                ),
                "payment_amount": pagamento.get(
                    "amount"
                ),
                "payment_status": pagamento.get(
                    "status"
                ),
                "payment_status_detail": pagamento.get(
                    "status_detail"
                ),
                "payment_method_id": (
                    pagamento.get(
                        "payment_method",
                        {}
                    ).get("id")
                    if isinstance(
                        pagamento.get(
                            "payment_method",
                            {}
                        ),
                        dict
                    )
                    else None
                ),
                "payment_method_type": (
                    pagamento.get(
                        "payment_method",
                        {}
                    ).get("type")
                    if isinstance(
                        pagamento.get(
                            "payment_method",
                            {}
                        ),
                        dict
                    )
                    else None
                ),
            }
        )


print(
    f"\nRegistros de pagamento encontrados: "
    f"{len(pagamentos)}"
)


# ============================================================
# DATAFRAME PARA ANÁLISE
# ============================================================

df = pd.DataFrame(pagamentos)

if df.empty:

    print(
        "\n⚠ Nenhum registro de pagamento "
        "foi encontrado."
    )

else:

    # Garantir que valores monetários sejam numéricos
    df["payment_amount"] = pd.to_numeric(
        df["payment_amount"],
        errors="coerce"
    )


# ============================================================
# QUALIDADE DOS DADOS
# ============================================================

print("\n" + "=" * 70)
print("5. QUALIDADE DOS DADOS")
print("=" * 70)

if df.empty:

    print(
        "\nNão foi possível realizar a análise "
        "dos registros."
    )

else:

    # --------------------------------------------------------
    # Valores ausentes
    # --------------------------------------------------------

    print("\nValores ausentes por coluna:")

    valores_ausentes = df.isna().sum()

    for coluna, quantidade in valores_ausentes.items():

        print(
            f"  {coluna}: {quantidade}"
        )

    # --------------------------------------------------------
    # Duplicidades
    # --------------------------------------------------------

    duplicados_order = df[
        "order_id"
    ].duplicated().sum()

    duplicados_payment = df[
        "payment_id"
    ].duplicated().sum()

    print("\nDuplicidades:")

    print(
        f"  Order IDs duplicados: "
        f"{duplicados_order}"
    )

    print(
        f"  Payment IDs duplicados: "
        f"{duplicados_payment}"
    )

    # --------------------------------------------------------
    # Estatísticas monetárias
    # --------------------------------------------------------

    print("\nEstatísticas dos pagamentos:")

    print(
        f"  Quantidade: "
        f"{len(df)}"
    )

    print(
        f"  Mínimo: "
        f"R$ {df['payment_amount'].min():.2f}"
    )

    print(
        f"  Máximo: "
        f"R$ {df['payment_amount'].max():.2f}"
    )

    print(
        f"  Média: "
        f"R$ {df['payment_amount'].mean():.2f}"
    )

    print(
        f"  Total: "
        f"R$ {df['payment_amount'].sum():.2f}"
    )


# ============================================================
# DISTRIBUIÇÃO DOS STATUS
# ============================================================

print("\n" + "=" * 70)
print("6. DISTRIBUIÇÃO DOS STATUS")
print("=" * 70)

if not df.empty:

    print("\nStatus das ordens:")

    print(
        df["order_status"]
        .value_counts(dropna=False)
        .to_string()
    )

    print("\nStatus dos pagamentos:")

    print(
        df["payment_status"]
        .value_counts(dropna=False)
        .to_string()
    )

    print("\nDetalhes dos pagamentos:")

    print(
        df["payment_status_detail"]
        .value_counts(dropna=False)
        .to_string()
    )


# ============================================================
# MÉTODOS DE PAGAMENTO
# ============================================================

print("\n" + "=" * 70)
print("7. MÉTODOS DE PAGAMENTO")
print("=" * 70)

if not df.empty:

    print("\nIDs dos métodos:")

    print(
        df["payment_method_id"]
        .value_counts(dropna=False)
        .to_string()
    )

    print("\nTipos dos métodos:")

    print(
        df["payment_method_type"]
        .value_counts(dropna=False)
        .to_string()
    )


# ============================================================
# VALIDAÇÕES FINAIS
# ============================================================

print("\n" + "=" * 70)
print("8. VALIDAÇÕES FINAIS")
print("=" * 70)

validacoes = {}

validacoes[
    "arquivos_lidos"
] = len(registros) == len(arquivos)

validacoes[
    "sem_erros_leitura"
] = len(erros_leitura) == 0

validacoes[
    "schema_consistente"
] = len(schemas) == 1

validacoes[
    "possui_pagamentos"
] = not df.empty

if not df.empty:

    validacoes[
        "sem_valores_ausentes"
    ] = not df.isna().any().any()

    validacoes[
        "sem_orders_duplicadas"
    ] = duplicados_order == 0

    validacoes[
        "sem_payments_duplicados"
    ] = duplicados_payment == 0

    validacoes[
        "valores_monetarios_validos"
    ] = not df[
        "payment_amount"
    ].isna().any()

else:

    validacoes[
        "sem_valores_ausentes"
    ] = False

    validacoes[
        "sem_orders_duplicadas"
    ] = False

    validacoes[
        "sem_payments_duplicados"
    ] = False

    validacoes[
        "valores_monetarios_validos"
    ] = False


for nome, resultado in validacoes.items():

    simbolo = "✓" if resultado else "✗"

    print(
        f"{simbolo} {nome}: "
        f"{'OK' if resultado else 'FALHA'}"
    )


# ============================================================
# GERAÇÃO DO RELATÓRIO
# ============================================================

print("\n" + "=" * 70)
print("9. GERAÇÃO DO RELATÓRIO")
print("=" * 70)

if not df.empty:

    df.to_csv(
        ARQUIVO_RELATORIO,
        index=False,
        encoding="utf-8-sig"
    )

    print(
        f"\n✓ Relatório gerado:"
    )

    print(
        f"  {ARQUIVO_RELATORIO}"
    )

else:

    print(
        "\n⚠ Relatório não foi gerado "
        "porque não existem registros."
    )


# ============================================================
# RESULTADO FINAL
# ============================================================

print("\n" + "=" * 70)
print("RESULTADO FINAL DA ANÁLISE")
print("=" * 70)

if all(validacoes.values()):

    print(
        "\n✓ CAMADA BRONZE VALIDADA COM SUCESSO."
    )

    print(
        "✓ Os dados apresentam estrutura "
        "consistente."
    )

    print(
        "✓ Nenhum erro de leitura foi encontrado."
    )

    print(
        "✓ Nenhuma duplicidade foi encontrada."
    )

    print(
        "✓ Nenhum valor ausente foi encontrado."
    )

else:

    print(
        "\n⚠ A CAMADA BRONZE POSSUI "
        "PONTOS A SEREM VERIFICADOS."
    )

print("\n" + "=" * 70)

