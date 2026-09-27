import json
import random
import uuid
from pathlib import Path

import requests


# ============================================================
# CONFIGURAÇÕES
# ============================================================

URL_BASE = "https://api.mercadopago.com"

URL_CARD_TOKEN = f"{URL_BASE}/v1/card_tokens"
URL_ORDERS = f"{URL_BASE}/v1/orders"

PASTA_BRONZE = Path(
    "/Volumes/tcc_unifor/bronze/raw_files/mercado_pago"
)

QUANTIDADE_TRANSACOES = 20

VALOR_MINIMO = 20.00
VALOR_MAXIMO = 1000.00


# ============================================================
# TOKEN DO MERCADO PAGO
# ============================================================

MERCADO_PAGO_ACCESS_TOKEN = dbutils.secrets.get(
    catalog="tcc_unifor",
    schema="config",
    key="mercado_pago_access_token"
)


HEADERS = {
    "Authorization": f"Bearer {MERCADO_PAGO_ACCESS_TOKEN}",
    "Content-Type": "application/json"
}


# ============================================================
# CARTÃO MASTERCARD DE TESTE
# ============================================================

CARD_NUMBER = "5480832801033311"
SECURITY_CODE = "123"

EXPIRATION_MONTH = 11
EXPIRATION_YEAR = 2030

CARDHOLDER_NAME = "APRO"

CARDHOLDER_IDENTIFICATION = {
    "type": "CPF",
    "number": "12345678909"
}


# ============================================================
# CRIAÇÃO DA PASTA BRONZE
# ============================================================

PASTA_BRONZE.mkdir(
    parents=True,
    exist_ok=True
)


# ============================================================
# FUNÇÃO — GERAR VALOR ALEATÓRIO
# ============================================================

def gerar_valor_aleatorio():
    return round(
        random.uniform(
            VALOR_MINIMO,
            VALOR_MAXIMO
        ),
        2
    )


# ============================================================
# FUNÇÃO — GERAR CARD TOKEN
# ============================================================

def gerar_card_token():

    payload = {
        "card_number": CARD_NUMBER,
        "security_code": SECURITY_CODE,
        "expiration_month": EXPIRATION_MONTH,
        "expiration_year": EXPIRATION_YEAR,
        "cardholder": {
            "name": CARDHOLDER_NAME,
            "identification": CARDHOLDER_IDENTIFICATION
        }
    }

    response = requests.post(
        URL_CARD_TOKEN,
        headers=HEADERS,
        json=payload,
        timeout=30
    )

    if response.status_code != 201:

        print()
        print("ERRO AO GERAR CARD TOKEN")
        print(f"HTTP Status: {response.status_code}")
        print(response.text)

        raise RuntimeError(
            "Não foi possível gerar o card token."
        )

    dados = response.json()

    card_token = dados.get("id")

    if not card_token:
        raise RuntimeError(
            "A API não retornou o card token."
        )

    return card_token


# ============================================================
# FUNÇÃO — CRIAR ORDER PIX
# ============================================================

def criar_order_pix(
    valor,
    external_reference
):

    payload = {
        "type": "online",
        "processing_mode": "automatic",
        "total_amount": f"{valor:.2f}",
        "external_reference": external_reference,
        "payer": {
            "email": "test@testuser.com"
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

    headers = {
        **HEADERS,
        "X-Idempotency-Key": str(uuid.uuid4())
    }

    response = requests.post(
        URL_ORDERS,
        headers=headers,
        json=payload,
        timeout=30
    )

    if response.status_code not in (200, 201):

        print()
        print("ERRO AO CRIAR ORDER PIX")
        print(f"HTTP Status: {response.status_code}")
        print(response.text)

        raise RuntimeError(
            "Não foi possível criar a Order Pix."
        )

    return response.json()


# ============================================================
# FUNÇÃO — CRIAR ORDER MASTERCARD
# ============================================================

def criar_order_mastercard(
    valor,
    external_reference,
    card_token
):

    payload = {
        "type": "online",
        "processing_mode": "automatic",
        "total_amount": f"{valor:.2f}",
        "external_reference": external_reference,
        "payer": {
            "email": "test@testuser.com"
        },
        "transactions": {
            "payments": [
                {
                    "amount": f"{valor:.2f}",
                    "payment_method": {
                        "id": "master",
                        "type": "credit_card",
                        "token": card_token,
                        "installments": 1
                    }
                }
            ]
        }
    }

    headers = {
        **HEADERS,
        "X-Idempotency-Key": str(uuid.uuid4())
    }

    response = requests.post(
        URL_ORDERS,
        headers=headers,
        json=payload,
        timeout=30
    )

    if response.status_code not in (200, 201):

        print()
        print("ERRO AO CRIAR ORDER MASTERCARD")
        print(f"HTTP Status: {response.status_code}")
        print(response.text)

        raise RuntimeError(
            "Não foi possível criar a Order Mastercard."
        )

    return response.json()


# ============================================================
# INÍCIO DA INGESTÃO
# ============================================================

print("=" * 70)
print("INGESTÃO — MERCADO PAGO")
print("=" * 70)

print()
print(f"Quantidade de transações: {QUANTIDADE_TRANSACOES}")
print(
    f"Faixa de valores: "
    f"R$ {VALOR_MINIMO:.2f} "
    f"até "
    f"R$ {VALOR_MAXIMO:.2f}"
)

print()
print("Métodos disponíveis:")
print(" - Pix")
print(" - Mastercard")


# ============================================================
# CONTADORES
# ============================================================

quantidade_pix = 0
quantidade_mastercard = 0

valor_total_pix = 0.00
valor_total_mastercard = 0.00

transacoes_processadas = []


# ============================================================
# GERAÇÃO DAS TRANSAÇÕES
# ============================================================

for numero in range(
    1,
    QUANTIDADE_TRANSACOES + 1
):

    print()
    print("-" * 70)
    print(
        f"TRANSAÇÃO {numero}/{QUANTIDADE_TRANSACOES}"
    )
    print("-" * 70)

    # --------------------------------------------------------
    # Escolher método aleatoriamente
    # --------------------------------------------------------

    metodo = random.choice(
        [
            "pix",
            "mastercard"
        ]
    )

    # --------------------------------------------------------
    # Gerar valor aleatório
    # --------------------------------------------------------

    valor = gerar_valor_aleatorio()

    external_reference = (
        f"TCC-ALEATORIO-"
        f"{uuid.uuid4().hex[:8].upper()}"
    )

    print(
        f"Método escolhido: "
        f"{metodo.upper()}"
    )

    print(
        f"Valor: "
        f"R$ {valor:.2f}"
    )

    print(
        f"Referência: "
        f"{external_reference}"
    )


    # ========================================================
    # PIX
    # ========================================================

    if metodo == "pix":

        dados_order = criar_order_pix(
            valor=valor,
            external_reference=external_reference
        )

        quantidade_pix += 1

        valor_total_pix += valor


    # ========================================================
    # MASTERCARD
    # ========================================================

    else:

        print(
            "Gerando card token..."
        )

        card_token = gerar_card_token()

        print(
            "Card token gerado com sucesso."
        )

        dados_order = criar_order_mastercard(
            valor=valor,
            external_reference=external_reference,
            card_token=card_token
        )

        quantidade_mastercard += 1

        valor_total_mastercard += valor


    # ========================================================
    # VALIDAR ID DA ORDER
    # ========================================================

    order_id = dados_order.get("id")

    if not order_id:

        print()
        print(
            "Resposta recebida pela API:"
        )

        print(
            json.dumps(
                dados_order,
                indent=4,
                ensure_ascii=False
            )
        )

        raise RuntimeError(
            "A API não retornou o ID da Order."
        )


    # ========================================================
    # SALVAR JSON BRUTO
    # ========================================================

    arquivo = (
        PASTA_BRONZE /
        f"order_{order_id}.json"
    )

    with open(
        arquivo,
        "w",
        encoding="utf-8"
    ) as f:

        json.dump(
            dados_order,
            f,
            indent=4,
            ensure_ascii=False
        )


    # ========================================================
    # RESULTADO DA TRANSAÇÃO
    # ========================================================

    status = dados_order.get(
        "status"
    )

    status_detail = dados_order.get(
        "status_detail"
    )

    print()
    print(
        f"Order criada: {order_id}"
    )

    print(
        f"Status: {status}"
    )

    print(
        f"Status detalhe: {status_detail}"
    )

    print(
        f"Arquivo salvo: {arquivo.name}"
    )


    # ========================================================
    # GUARDAR RESUMO
    # ========================================================

    transacoes_processadas.append(
        {
            "numero": numero,
            "metodo": metodo,
            "valor": valor,
            "order_id": order_id,
            "status": status,
            "status_detail": status_detail
        }
    )


# ============================================================
# RESUMO FINAL
# ============================================================

valor_total = (
    valor_total_pix +
    valor_total_mastercard
)

quantidade_total = (
    quantidade_pix +
    quantidade_mastercard
)

valor_medio = (
    valor_total / quantidade_total
    if quantidade_total > 0
    else 0
)


print()
print()
print("=" * 70)
print("RESUMO DA INGESTÃO")
print("=" * 70)

print()
print(
    f"Total de transações: "
    f"{quantidade_total}"
)

print()
print("PIX")
print(
    f"Quantidade: "
    f"{quantidade_pix}"
)

print(
    f"Valor total: "
    f"R$ {valor_total_pix:.2f}"
)

print()
print("MASTERCARD")
print(
    f"Quantidade: "
    f"{quantidade_mastercard}"
)

print(
    f"Valor total: "
    f"R$ {valor_total_mastercard:.2f}"
)

print()
print("TOTAL")

print(
    f"Quantidade: "
    f"{quantidade_total}"
)

print(
    f"Valor total: "
    f"R$ {valor_total:.2f}"
)

print(
    f"Valor médio: "
    f"R$ {valor_medio:.2f}"
)


# ============================================================
# DISTRIBUIÇÃO
# ============================================================

print()
print("=" * 70)
print("DISTRIBUIÇÃO DOS MÉTODOS")
print("=" * 70)

if quantidade_total > 0:

    percentual_pix = (
        quantidade_pix /
        quantidade_total *
        100
    )

    percentual_mastercard = (
        quantidade_mastercard /
        quantidade_total *
        100
    )

    print(
        f"Pix: "
        f"{quantidade_pix} "
        f"({percentual_pix:.1f}%)"
    )

    print(
        f"Mastercard: "
        f"{quantidade_mastercard} "
        f"({percentual_mastercard:.1f}%)"
    )


# ============================================================
# TABELA RESUMIDA DAS TRANSAÇÕES
# ============================================================

print()
print("=" * 70)
print("TRANSAÇÕES GERADAS")
print("=" * 70)

for transacao in transacoes_processadas:

    print(
        f"{transacao['numero']:02d} | "
        f"{transacao['metodo'].upper():12s} | "
        f"R$ {transacao['valor']:8.2f} | "
        f"{transacao['status']:16s} | "
        f"{transacao['order_id']}"
    )


# ============================================================
# FINAL
# ============================================================

print()
print("=" * 70)
print("INGESTÃO CONCLUÍDA COM SUCESSO")
print("=" * 70)
