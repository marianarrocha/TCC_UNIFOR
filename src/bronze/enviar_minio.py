import os
from pathlib import Path
from datetime import datetime

from dotenv import load_dotenv
from minio import Minio
from minio.error import S3Error


# ============================================================
# CONFIGURAÇÃO
# ============================================================

# Carrega as variáveis do arquivo .env
load_dotenv()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")

MINIO_SECURE = (
    os.getenv("MINIO_SECURE", "False").lower() == "true"
)

# Diretório local contendo os dados brutos
PASTA_BRONZE = (
    Path(__file__).resolve().parents[2]
    / "data"
    / "bronze"
    / "mercado_pago"
)


# ============================================================
# VALIDAÇÃO DAS CONFIGURAÇÕES
# ============================================================

variaveis_obrigatorias = {
    "MINIO_ENDPOINT": MINIO_ENDPOINT,
    "MINIO_ACCESS_KEY": MINIO_ACCESS_KEY,
    "MINIO_SECRET_KEY": MINIO_SECRET_KEY,
    "MINIO_BUCKET": MINIO_BUCKET,
}

for nome, valor in variaveis_obrigatorias.items():

    if not valor:
        raise ValueError(
            f"{nome} não foi encontrado no arquivo .env"
        )


# ============================================================
# INÍCIO
# ============================================================

print("=" * 70)
print("ENVIO DA CAMADA BRONZE PARA O MINIO")
print("=" * 70)

print(f"\nEndpoint MinIO: {MINIO_ENDPOINT}")
print(f"Bucket:         {MINIO_BUCKET}")
print(f"Origem:         {PASTA_BRONZE}")


# ============================================================
# VALIDAÇÃO DO DIRETÓRIO LOCAL
# ============================================================

if not PASTA_BRONZE.exists():

    raise FileNotFoundError(
        "Diretório da camada Bronze não encontrado:\n"
        f"{PASTA_BRONZE}"
    )

if not PASTA_BRONZE.is_dir():

    raise NotADirectoryError(
        "O caminho da camada Bronze não é um diretório:\n"
        f"{PASTA_BRONZE}"
    )


# ============================================================
# CONEXÃO COM O MINIO
# ============================================================

print("\n" + "=" * 70)
print("CONECTANDO AO MINIO")
print("=" * 70)

cliente_minio = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=MINIO_SECURE,
)

try:

    if cliente_minio.bucket_exists(MINIO_BUCKET):

        print(
            f"\n✓ Bucket '{MINIO_BUCKET}' "
            "encontrado."
        )

    else:

        cliente_minio.make_bucket(MINIO_BUCKET)

        print(
            f"\n✓ Bucket '{MINIO_BUCKET}' "
            "criado com sucesso."
        )

except S3Error as erro:

    raise RuntimeError(
        "Não foi possível acessar o MinIO.\n"
        f"Detalhes: {erro}"
    )


# ============================================================
# LOCALIZAÇÃO DOS ARQUIVOS JSON
# ============================================================

arquivos = sorted(
    PASTA_BRONZE.glob("*.json")
)

print("\n" + "=" * 70)
print("ARQUIVOS DA CAMADA BRONZE")
print("=" * 70)

print(
    f"\nArquivos JSON encontrados: {len(arquivos)}"
)

if not arquivos:

    raise FileNotFoundError(
        "Nenhum arquivo JSON foi encontrado em:\n"
        f"{PASTA_BRONZE}"
    )


# ============================================================
# DATA DE PARTIÇÃO
# ============================================================
#
# A data utilizada na estrutura do objeto é a data da
# execução do processo.
#
# Exemplo:
#
# mercado_pago/
# └── ano=2026/
#     └── mes=09/
#         └── dia=07/
#             └── order_001.json
#
# ============================================================

agora = datetime.now()

ano = agora.strftime("%Y")
mes = agora.strftime("%m")
dia = agora.strftime("%d")


# ============================================================
# ENVIO DOS ARQUIVOS
# ============================================================

print("\n" + "=" * 70)
print("ENVIANDO ARQUIVOS PARA O MINIO")
print("=" * 70)

sucessos = 0
erros = 0


for arquivo in arquivos:

    nome_arquivo = arquivo.name

    # Caminho do objeto dentro do bucket
    object_name = (
        f"mercado_pago/"
        f"ano={ano}/"
        f"mes={mes}/"
        f"dia={dia}/"
        f"{nome_arquivo}"
    )

    try:

        # ----------------------------------------------------
        # Validação básica do arquivo
        # ----------------------------------------------------

        tamanho = arquivo.stat().st_size

        if tamanho == 0:

            raise ValueError(
                "Arquivo vazio."
            )

        # ----------------------------------------------------
        # Upload para o MinIO
        # ----------------------------------------------------

        cliente_minio.fput_object(
            bucket_name=MINIO_BUCKET,
            object_name=object_name,
            file_path=str(arquivo),
            content_type="application/json",
        )

        sucessos += 1

        print(
            f"\n✓ {nome_arquivo}"
        )

        print(
            f"  → {MINIO_BUCKET}/{object_name}"
        )

        print(
            f"  Tamanho: {tamanho:,} bytes"
        )

    except Exception as erro:

        erros += 1

        print(
            f"\n✗ Erro ao enviar {nome_arquivo}"
        )

        print(
            f"  Motivo: {erro}"
        )


# ============================================================
# RESUMO FINAL
# ============================================================

print("\n" + "=" * 70)
print("RESUMO DO PROCESSAMENTO")
print("=" * 70)

print(
    f"\nArquivos encontrados: {len(arquivos)}"
)

print(
    f"Arquivos enviados:   {sucessos}"
)

print(
    f"Arquivos com erro:   {erros}"
)

print(
    f"\nDestino:"
    f"\n  {MINIO_BUCKET}/"
    f"mercado_pago/"
    f"ano={ano}/"
    f"mes={mes}/"
    f"dia={dia}/"
)


# ============================================================
# VALIDAÇÃO DO RESULTADO
# ============================================================

if erros == 0:

    print(
        "\n✓ PROCESSAMENTO CONCLUÍDO COM SUCESSO."
    )

    print(
        "✓ Todos os arquivos da camada Bronze "
        "foram enviados para o MinIO."
    )

else:

    print(
        "\n⚠ PROCESSAMENTO CONCLUÍDO COM ERROS."
    )

    print(
        "⚠ Verifique os arquivos indicados acima."
    )

print("\n" + "=" * 70)
