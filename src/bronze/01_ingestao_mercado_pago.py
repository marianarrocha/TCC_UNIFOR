import json
import uuid
from pathlib import Path

import requests


# ============================================================
# CONFIGURAÇÃO
# ============================================================

# Volume do Unity Catalog
PASTA_BRONZE = Path(
    "/Volumes/tcc_unifor/bronze/raw_files/mercado_pago"
)

PASTA_BRONZE.mkdir(
    parents=True,
    exist_ok=True
)


# ============================================================
# CREDENCIAIS
# ============================================================

MERCADO_PAGO_ACCESS_TOKEN = dbutils.secrets.get(
    catalog="tcc_unifor",
    schema="config",
    key="mercado_pago_access_token"
)

if not MERCADO_PAGO_ACCESS_TOKEN:
    raise ValueError(
        "Variável MERCADO_PAGO_ACCESS_TOKEN não encontrada."
    )


# ============================================================
# API
# ============================================================

URL_API = "https://api.mercadopago.com/v1/orders"


# ============================================================
# VALORES DOS PEDIDOS DE TESTE
# ============================================================

VALORES = [
    25.90,
    50.00,
    75.50,
    100.00,
    120.90,
    150.00,
    200.00,
    250.75,
    350.00,
    500.00,
]


# ============================================================
# CABEÇALHOS
# ============================================================

headers = {
    "Authorization": f"Bearer {MERCADO_PAGO_ACCESS_TOKEN}",
    "Content-Type": "application/json",
}


# ============================================================
# INÍCIO
# ============================================================

print("=" * 70)
print("INGESTÃO DE TRANSAÇÕES — MERCADO PAGO")
print("=" * 70)

print(f"\nEndpoint: POST {URL_API}")
print(f"\nDestino Bronze: {PASTA_BRONZE}")
print(f"\nQuantidade de transações: {len(VALORES)}")


sucessos = 0
erros = 0
arquivos_criados = []


# ============================================================
# PROCESSAMENTO
# ============================================================

for indice, valor in enumerate(VALORES, start=1):

    external_reference = (
        f"TCC-TESTE-{indice:03d}"
    )

    payload = {
        "type": "online",
        "processing_mode": "automatic",
        "total_amount": f"{valor:.2f}",
        "external_reference": external_reference,
        "payer": {
            "email": "test_user_br@testuser.com"
        },
        "transactions": {
            "payments": [
                {
                    "amount": f"{valor:.2f}",
                    "payment_method": {
                        "id": "pix",
                        "type": "bank_transfer"
                    }
                }
            ]
        }
    }

    # ========================================================
    # IDEMPOTÊNCIA
    # ========================================================

    idempotency_key = str(uuid.uuid4())

    headers["X-Idempotency-Key"] = (
        idempotency_key
    )

    print("\n" + "-" * 70)

    print(
        f"Transação {indice:02d}/{len(VALORES)}"
    )

    print(
        f"Valor: R$ {valor:.2f}"
    )

    print(
        f"Referência: {external_reference}"
    )

    try:

        # ====================================================
        # REQUISIÇÃO À API
        # ====================================================

        response = requests.post(
            URL_API,
            headers=headers,
            json=payload,
            timeout=30
        )

        # ====================================================
        # SUCESSO
        # ====================================================

        if response.ok:

            dados = response.json()

            order_id = dados.get("id")

            # ------------------------------------------------
            # VALIDAÇÃO DO ID RETORNADO
            # ------------------------------------------------

            if not order_id:

                erros += 1

                print(
                    "✗ A API respondeu com sucesso, "
                    "mas não retornou o Order ID."
                )

                continue

            # ------------------------------------------------
            # NOME ÚNICO DO ARQUIVO
            # ------------------------------------------------

            nome_arquivo = (
                f"order_{order_id}.json"
            )

            caminho_arquivo = (
                PASTA_BRONZE
                / nome_arquivo
            )

            # ------------------------------------------------
            # PROTEÇÃO CONTRA SOBRESCRITA
            # ------------------------------------------------

            if caminho_arquivo.exists():

                print(
                    "⚠ Arquivo já existe."
                )

                print(
                    f"  Arquivo: {nome_arquivo}"
                )

                print(
                    "  Registro não será sobrescrito."
                )

                continue

            # ------------------------------------------------
            # GRAVAÇÃO DO JSON BRUTO
            # ------------------------------------------------

            with open(
                caminho_arquivo,
                "w",
                encoding="utf-8"
            ) as arquivo:

                json.dump(
                    dados,
                    arquivo,
                    ensure_ascii=False,
                    indent=2
                )

            sucessos += 1

            arquivos_criados.append(
                nome_arquivo
            )

            print(
                "✓ Requisição realizada com sucesso"
            )

            print(
                f"  HTTP: {response.status_code}"
            )

            print(
                f"  Order ID: {order_id}"
            )

            print(
                f"  Status: {dados.get('status')}"
            )

            print(
                f"  Arquivo: {nome_arquivo}"
            )

        # ====================================================
        # ERRO DA API
        # ====================================================

        else:

            erros += 1

            print(
                "✗ Erro retornado pela API"
            )

            print(
                f"  HTTP: {response.status_code}"
            )

            try:

                erro_api = response.json()

                print(
                    json.dumps(
                        erro_api,
                        ensure_ascii=False,
                        indent=2
                    )
                )

            except ValueError:

                print(
                    response.text
                )

    # ========================================================
    # TIMEOUT
    # ========================================================

    except requests.exceptions.Timeout:

        erros += 1

        print(
            "✗ Timeout na requisição."
        )

    # ========================================================
    # ERRO DE COMUNICAÇÃO
    # ========================================================

    except requests.exceptions.RequestException as erro:

        erros += 1

        print(
            "✗ Erro de comunicação com a API."
        )

        print(
            f"  {erro}"
        )

    # ========================================================
    # ERRO AO INTERPRETAR RESPOSTA
    # ========================================================

    except ValueError as erro:

        erros += 1

        print(
            "✗ Resposta inválida da API."
        )

        print(
            f"  {erro}"
        )


# ============================================================
# RESUMO
# ============================================================

print("\n" + "=" * 70)
print("RESUMO DA INGESTÃO")
print("=" * 70)

print(
    f"\nTransações solicitadas: {len(VALORES)}"
)

print(
    f"Transações armazenadas: {sucessos}"
)

print(
    f"Transações com erro: {erros}"
)

print(
    f"\nNovos arquivos criados: {len(arquivos_criados)}"
)

print(
    f"\nDiretório Bronze: {PASTA_BRONZE}"
)


# ============================================================
# ARQUIVOS CRIADOS
# ============================================================

if arquivos_criados:

    print("\nArquivos armazenados:")

    for arquivo in arquivos_criados:

        print(
            f"  ✓ {arquivo}"
        )


# ============================================================
# CONCLUSÃO
# ============================================================

if erros == 0:

    print(
        "\n✓ INGESTÃO CONCLUÍDA COM SUCESSO."
    )

else:

    print(
        "\n⚠ INGESTÃO CONCLUÍDA COM ERROS."
    )

print("\n" + "=" * 70)