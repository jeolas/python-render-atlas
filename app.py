import os
from datetime import datetime

from pymongo import MongoClient

# Lê variáveis de ambiente
MONGO_URI = os.environ["MONGO_URI"]
DB_NAME = os.environ.get("DB_NAME", "marketing_db")

def main():
    # Conecta no MongoDB Atlas
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]

    colecao_logs = db["logs_execucao"]

    agora = datetime.utcnow()

    doc = {
        "executado_em": agora,
        "mensagem": "Job rodou com sucesso via Render + Atlas.",
    }

    colecao_logs.insert_one(doc)

    print("Registro inserido no MongoDB Atlas:", doc)

if __name__ == "__main__":
    main()
