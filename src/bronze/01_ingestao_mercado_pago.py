import json
import random
import uuid
from pathlib import Path

import requests

# CONFIGURAÇÕES

VOLUME_RAW = "/Volumes/tcc_unifor/bronze/raw_files/mercado_pago"

QUANTIDADE_TRANSACOES = 10

VALOR_MINIMO = 20.00
VALOR_MAXIMO = 1000.00

URL_API = "https://api.mercadopago.com/v1/orders"
URL_CARD_TOKEN = "https://api.mercadopago.com/v1/card_tokens"


# ACCESS TOKEN

MERCADO_PAGO_ACCESS_TOKEN = dbutils.secrets.get(
    catalog="tcc_unifor",
    schema="config",
    key="mercado_pago_access_token"
)

HEADERS = {
    "Authorization": f"Bearer {MERCADO_PAGO_ACCESS_TOKEN}",
    "Content-Type": "application/json"
}

# Adicionar pagamento em cartão

CARTOES_TESTE = {
    "mastercard": {
        "nome": "Mastercard",
        "payment_method_id": "master",
        "numero": "5480832801033311",
        "codigo_seguranca": "123",
        "mes_expiracao": 11,
        "ano_expiracao": 2030
    },

    "visa": {
        "nome": "Visa",
        "payment_method_id": "visa",
        "numero": "4235647728025682",
        "codigo_seguranca": "123",
        "mes_expiracao": 11,
        "ano_expiracao": 2030
    }
}

# Gerar valores aleatórios para transações e gerar token de cartão mercado pago visa e master

def gerar_valor_aleatorio():
    
    return round(
        random.uniform(
            VALOR_MINIMO,
            VALOR_MAXIMO
        ),
        2
    )


def gerar_card_token(cartao):

    payload = {
        "card_number": cartao["numero"],
        "expiration_month": cartao["mes_expiracao"],
        "expiration_year": cartao["ano_expiracao"],
        "security_code": cartao["codigo_seguranca"],
        "cardholder": {
            "name": "APRO",
            "identification": {
                "type": "CPF",
                "number": "12345678909"
            }
        }
    }

    response = requests.post(
        URL_CARD_TOKEN,
        headers=HEADERS,
        json=payload
    )

    if response.status_code != 201:
        print("ERRO AO GERAR CARD TOKEN")
        print("Status:", response.status_code)
        print("Resposta:", response.text)

        raise Exception(
            f"Erro ao gerar token do cartão: "
            f"{response.status_code}"
        )

    dados_token = response.json()

    print("Card token gerado com sucesso.")

    return dados_token["id"]


def criar_order_pix(valor, external_reference):
    """
    Cria uma order utilizando Pix.
    """

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

    headers = HEADERS.copy()

    headers["X-Idempotency-Key"] = str(uuid.uuid4())

    response = requests.post(
        URL_API,
        headers=headers,
        json=payload
    )

    return response


def criar_order_cartao(
    valor,
    external_reference,
    cartao
):

    card_token = gerar_card_token(cartao)


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
                        "id": cartao["payment_method_id"],
                        "type": "credit_card",
                        "token": card_token,
                        "installments": 1
                    }
                }
            ]
        }
    }

    headers = HEADERS.copy()

    headers["X-Idempotency-Key"] = str(uuid.uuid4())

    response = requests.post(
        URL_API,
        headers=headers,
        json=payload
    )

    return response


# DIRETÓRIO

Path(VOLUME_RAW).mkdir(
    parents=True,
    exist_ok=True
)


# INGESTÃO

print("=" * 70)
print("INGESTÃO MERCADO PAGO")
print("=" * 70)

print(f"Quantidade de transações: {QUANTIDADE_TRANSACOES}")
print(
    f"Faixa de valores: "
    f"R$ {VALOR_MINIMO:.2f} "
    f"até R$ {VALOR_MAXIMO:.2f}"
)

print("Métodos disponíveis: Pix, Mastercard e Visa")

print("=" * 70)


resultados = []

valores = []

quantidade_pix = 0
quantidade_mastercard = 0
quantidade_visa = 0

# LOOP PRINCIPAL

for i in range(QUANTIDADE_TRANSACOES):

    print()
    print("-" * 70)
    print(f"TRANSAÇÃO {i + 1}/{QUANTIDADE_TRANSACOES}")
    print("-" * 70)

    # Gera valor aleatório

    valor = gerar_valor_aleatorio()

    valores.append(valor)

    # Escolhe aleatoriamente o método

    metodo = random.choice([
        "pix",
        "mastercard",
        "visa"
    ])

    # Gera referência externa

    external_reference = (
        f"TCC-ALEATORIO-"
        f"{uuid.uuid4().hex[:8].upper()}"
    )

    print(f"Valor: R$ {valor:.2f}")
    print(f"Método: {metodo}")
    print(f"Referência: {external_reference}")

    # PIX

    if metodo == "pix":

        response = criar_order_pix(
            valor=valor,
            external_reference=external_reference
        )

        quantidade_pix += 1

    # MASTERCARD

    elif metodo == "mastercard":

        response = criar_order_cartao(
            valor=valor,
            external_reference=external_reference,
            cartao=CARTOES_TESTE["mastercard"]
        )

        quantidade_mastercard += 1

    # VISA

    elif metodo == "visa":

        response = criar_order_cartao(
            valor=valor,
            external_reference=external_reference,
            cartao=CARTOES_TESTE["visa"]
        )

        quantidade_visa += 1

    # Verificar resposta

    print(f"HTTP Status: {response.status_code}")

    if response.status_code not in [200, 201]:

        print("ERRO AO CRIAR ORDER")
        print(response.text)

        continue

    dados_order = response.json()

    order_id = dados_order.get("id")

    print(f"Order ID: {order_id}")
    print(
        f"Status: "
        f"{dados_order.get('status')}"
    )

    print(
        f"Status detail: "
        f"{dados_order.get('status_detail')}"
    )

    # Salva JSON bruto

    caminho_arquivo = (
        Path(VOLUME_RAW)
        / f"order_{order_id}.json"
    )

    with open(
        caminho_arquivo,
        "w",
        encoding="utf-8"
    ) as arquivo:

        json.dump(
            dados_order,
            arquivo,
            ensure_ascii=False,
            indent=4
        )

    print(f"Arquivo salvo em:")
    print(caminho_arquivo)

    # Guarda resultado

    resultados.append({
        "order_id": order_id,
        "valor": valor,
        "metodo": metodo,
        "status": dados_order.get("status"),
        "status_detail": dados_order.get("status_detail"),
        "external_reference": external_reference
    })

# RESUMO

print()
print()
print("=" * 70)
print("RESUMO DA INGESTÃO")
print("=" * 70)

print(
    f"Transações solicitadas: "
    f"{QUANTIDADE_TRANSACOES}"
)

print(
    f"Transações criadas com sucesso: "
    f"{len(resultados)}"
)

print()
print("Distribuição por método:")
print(
    f"  Pix:        {quantidade_pix}"
)
print(
    f"  Mastercard: {quantidade_mastercard}"
)
print(
    f"  Visa:       {quantidade_visa}"
)

print()

if valores:

    print(
        f"Valor mínimo gerado: "
        f"R$ {min(valores):.2f}"
    )

    print(
        f"Valor máximo gerado: "
        f"R$ {max(valores):.2f}"
    )

    print(
        f"Valor total gerado: "
        f"R$ {sum(valores):.2f}"
    )

    print(
        f"Valor médio gerado: "
        f"R$ {sum(valores) / len(valores):.2f}"
    )

print("=" * 70)
print("INGESTÃO FINALIZADA")
print("=" * 70)
