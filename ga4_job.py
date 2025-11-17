def carregar_ga4_para_mongo(db):
    # Credenciais GA4 vindas de variável de ambiente
    creds_json = os.environ["GA4_CREDS_JSON"]
    property_id = os.environ["GA4_PROPERTY_ID"]

    # Salva o JSON em arquivo temporário e configura a lib
    with open("ga4_key.json", "w") as f:
        f.write(creds_json)

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "ga4_key.json"

    client = BetaAnalyticsDataClient()

    # Dimensões que você pediu:
    # - eventName (pra ver session_start, user_engagement, view_item, add_to_cart, form_start, purchase)
    # - sessionSource / sessionMedium / sessionCampaignName (UTMs da sessão)
    # - date (pra ter série histórica)
    dimensions = [
        Dimension(name="date"),
        Dimension(name="eventName"),
        Dimension(name="sessionSource"),        # utm_source
        Dimension(name="sessionMedium"),        # utm_medium
        Dimension(name="sessionCampaignName"),  # utm_campaign
    ]

    # Métricas:
    # - eventCount           = contagem de eventos
    # - ecommercePurchases   = compras de e-commerce
    # - purchaseRevenue      = receita de compra
    metrics = [
        Metric(name="eventCount"),
        Metric(name="ecommercePurchases"),
        Metric(name="purchaseRevenue"),
    ]

    # Filtro para pegar só os eventos que você listou
    eventos_interesse = [
        "session_start",
        "user_engagement",
        "view_item",
        "add_to_cart",
        "form_start",
        "purchase",  # normalmente o evento de compra é "purchase"
    ]

    request = RunReportRequest(
        property=f"properties/{property_id}",
        dimensions=dimensions,
        metrics=metrics,
        date_ranges=[
            DateRange(start_date="7daysAgo", end_date="yesterday")
        ],
        # dimension_filter: eventName IN (…)
        dimension_filter=FilterExpression(
            filter=Filter(
                field_name="eventName",
                in_list_filter=Filter.InListFilter(values=eventos_interesse),
            )
        ),
    )

    response = client.run_report(request)

    linhas = []
    for row in response.rows:
        dim = row.dimension_values
        met = row.metric_values

        linhas.append({
            "date": dim[0].value,
            "event_name": dim[1].value,
            "utm_source": dim[2].value,
            "utm_medium": dim[3].value,
            "utm_campaign": dim[4].value,
            "event_count": int(met[0].value or 0),
            "ecommerce_purchases": float(met[1].value or 0),
            "purchase_revenue": float(met[2].value or 0),
            "importado_em": datetime.utcnow(),
        })

    if not linhas:
        print("GA4: nenhum dado retornado com os filtros informados.")
        return

    colecao = db["ga4_eventos_utms"]
    colecao.insert_many(linhas)
    print(f"GA4: inseridas {len(linhas)} linhas em ga4_eventos_utms.")
