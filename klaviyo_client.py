"""
Exemplo simples de integração com a API v2024-10-15 da Klaviyo.
Usa a chave pk_ fornecida para listar perfis e criar um evento de teste.
"""
import json
from typing import Any, Dict, Optional

import requests

BASE_URL = "https://a.klaviyo.com/api"
API_KEY = "pk_7addeaae261d460507141847f2523f885b"
REVISION = "2024-10-15"

HEADERS = {
    "Accept": "application/json",
    "Authorization": f"Klaviyo-API-Key {API_KEY}",
    "Revision": REVISION,
}


def list_profiles(page_size: int = 10) -> Dict[str, Any]:
    """Retorna um dicionário com a página inicial de perfis.

    Args:
        page_size: quantidade máxima de itens por página.
    """
    params = {"page[size]": page_size}
    response = requests.get(f"{BASE_URL}/profiles/", headers=HEADERS, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def create_custom_event(
    metric_name: str,
    profile_email: str,
    value: Optional[float] = None,
    properties: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Cria um evento personalizado associado a um e-mail.

    Os eventos são úteis para alimentar fluxos e segmentações dentro da Klaviyo.
    A chamada segue o formato descrito na documentação oficial.
    """
    data = {
        "data": {
            "type": "event",
            "attributes": {
                "metric": {"data": {"type": "metric", "attributes": {"name": metric_name}}},
                "customer_profile": {
                    "data": {
                        "type": "profile",
                        "attributes": {"email": profile_email},
                    }
                },
                "properties": properties or {},
                "value": value,
                "time": None,
            },
        }
    }

    response = requests.post(
        f"{BASE_URL}/events/",
        headers={**HEADERS, "Content-Type": "application/json"},
        data=json.dumps(data),
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


if __name__ == "__main__":
    print("Listando os 5 primeiros perfis...")
    perfis = list_profiles(page_size=5)
    print(json.dumps(perfis, indent=2, ensure_ascii=False))

    print("\nCriando evento de teste 'Teste de API'...")
    evento = create_custom_event(
        metric_name="Teste de API",
        profile_email="exemplo@dominio.com",
        value=1.0,
        properties={"origem": "script local", "descricao": "Exemplo rápido"},
    )
    print(json.dumps(evento, indent=2, ensure_ascii=False))
