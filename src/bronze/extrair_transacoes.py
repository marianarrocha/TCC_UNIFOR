import json
import os
import uuid
from pathlib import Path

import requests
from dotenv import load_dotenv


# ============================================================
# CONFIGURAÇÃO
# ============================================================

# Diretório raiz do projeto
RAIZ_PROJETO = Path(__file__).resolve().parents[2]

# Carrega as variáveis do arquivo .env
load_dotenv(RAIZ_PROJETO / ".env")


# ============================================================
# CREDENCIAIS
# ============================================================

MERCADO_PAGO_ACCESS_TOKEN = os.getenv(
    "MERCADO_PAGO_ACCESS_TOKEN"
)

if not MERCADO_PAGO_ACCESS_TOKEN:

    raise ValueError(
        "MERCADO_PAGO_ACCESS_TOKEN não foi "
        "encontrado no arquivo .env"
    )


# ============================================================
# API
# ============================================================

URL_API = (
    "https://api.mercadopago.com/v1/orders"
)

# Diretório onde os dados brutos serão armazenados
PASTA_BRONZE = (
    RAIZ_PROJETO
    / "data"
    / "bronze"
    / "mercado_pago"
)


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
# CRIAÇÃO DO DIRETÓRIO
# ============================================================

PASTA_BRONZE.mkdir(
    parents=True,
    exist_ok=True
)


# ============================================================
# CABEÇALHOS DA REQUISIÇÃO
# ============================================================

headers = {
    "Authorization": (
        f"Bearer {MERCADO_PAGO_ACCESS_TOKEN}"
    ),
    "Content-Type": "application/json",
}


# ============================================================
# INÍCIO DA INGESTÃO
# ============================================================

print("=" * 70)
print("INGESTÃO DE TRANSAÇÕES — MERCADO PAGO")
print("=" * 70)

print("\nEndpoint:")
print(f"  POST {URL_API}")

print("\nDestino local:")
print(f"  {PASTA_BRONZE}")

print("\nQuantidade de transações:")
print(f"  {len(VALORES)}")


# ============================================================
# PROCESSAMENTO
# ============================================================

sucessos = 0
erros = 0


for indice, valor in enumerate(VALORES, start=1):

    nome_arquivo = (
        f"order_{indice:03d}.json"
    )

    caminho_arquivo = (
        PASTA_BRONZE
        / nome_arquivo
    )

    external_reference = (
        f"TCC-TESTE-{indice:03d}"
    )

    # --------------------------------------------------------
    # Payload utilizado pela Orders API
    # --------------------------------------------------------

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

    # --------------------------------------------------------
    # Idempotência
    # --------------------------------------------------------

    idempotency_key = str(
        uuid.uuid4()
    )

    headers["X-Idempotency-Key"] = (
        idempotency_key
    )

    # --------------------------------------------------------
    # Requisição
    # --------------------------------------------------------

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

        response = requests.post(
            URL_API,
            headers=headers,
            json=payload,
            timeout=30
        )

        # ----------------------------------------------------
        # Verificação da resposta HTTP
        # ----------------------------------------------------

        if response.ok:

            dados = response.json()

            # ------------------------------------------------
            # Salvamento da resposta bruta
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

            print(
                f"✓ Requisição realizada com sucesso"
            )

            print(
                f"  HTTP: {response.status_code}"
            )

            print(
                f"  Order ID: "
                f"{dados.get('id')}"
            )

            print(
                f"  Status: "
                f"{dados.get('status')}"
            )

            print(
                f"  Arquivo: "
                f"{nome_arquivo}"
            )

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
                    "  Resposta:"
                )

                print(
                    json.dumps(
                        erro_api,
                        ensure_ascii=False,
                        indent=2
                    )
                )

            except ValueError:

                print(
                    "  Resposta:"
                )

                print(
                    response.text
                )

    except requests.exceptions.Timeout:

        erros += 1

        print(
            "✗ Timeout na requisição."
        )

    except requests.exceptions.RequestException as erro:

        erros += 1

        print(
            "✗ Erro de comunicação com a API."
        )

        print(
            f"  {erro}"
        )

    except ValueError as erro:

        erros += 1

        print(
            "✗ A API retornou uma resposta "
            "que não pôde ser interpretada como JSON."
        )

        print(
            f"  {erro}"
        )


# ============================================================
# RESUMO DA INGESTÃO
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
    f"Transações com erro:    {erros}"
)

print(
    f"\nDiretório Bronze:"
)

print(
    f"  {PASTA_BRONZE}"
)


# ============================================================
# RESULTADO FINAL
# ============================================================

if erros == 0:

    print(
        "\n✓ INGESTÃO CONCLUÍDA COM SUCESSO."
    )

    print(
        "✓ Todas as respostas da API foram "
        "armazenadas como JSON bruto."
    )

else:

    print(
        "\n⚠ INGESTÃO CONCLUÍDA COM ERROS."
    )

    print(
        "⚠ Verifique as requisições que "
        "apresentaram falha."
    )

print("\n" + "=" * 70)
