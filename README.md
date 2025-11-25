# python-render-atlas

Scripts de exemplo usados para integrações de marketing (MongoDB Atlas, GA4 e Klaviyo).

## Klaviyo

O arquivo `klaviyo_client.py` mostra chamadas REST usando a chave pública
`pk_7addeaae261d460507141847f2523f885b`.

### Como usar
1. Instale dependências: `pip install -r requirements.txt`.
2. Rode o script: `python klaviyo_client.py`.
   - Ele lista os primeiros perfis (`/profiles`) e envia um evento personalizado (`/events`).
   - Ajuste o e-mail do perfil para um contato existente antes de criar o evento.

A documentação oficial está em https://developers.klaviyo.com/en/reference/api_overview.
