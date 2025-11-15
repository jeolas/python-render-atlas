import os
from datetime import datetime
import json

from pymongo import MongoClient
import pandas as pd

# GA4
from google.analytics.data_v1beta import (
    BetaAnalyticsDataClient,
    RunReportRequest,
    DateRange,
    Dimension,
    Metric,
    Filter,
    FilterExpression,
)

# PDF
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.platypus import (
    SimpleDocTemplate,
    Table,
    TableStyle,
    Paragraph,
    Spacer,
)
from reportlab.lib.styles import getSampleStyleSheet

# Email
import smtplib
from email.message import EmailMessage


# --------- MONGO --------- #

def conectar_mongo():
    mongo_uri = os.environ["MONGO_URI"]
    db_name = os.environ.get("DB_NAME", "marketing_db")
    client = MongoClient(mongo_uri)
    return client[db_name]


# --------- GA4: COLETA E SALVA NO MONGO --------- #

def importar_ga4_eventos_para_mongo(db):
    creds_json = os.environ["GA4_CREDS_JSON"]
    property_id = os.environ["GA4_PROPERTY_ID"]

    # grava o JSON num arquivo temporário
    with open("ga4_key.json", "w", encoding="utf-8") as f:
        json.dump(creds_json, f)

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "ga4_key.json"

    client = BetaAnalyticsDataClient()

    dimensions = [
        Dimension(name="date"),
        Dimension(name="eventName"),
        Dimension(name="sessionSource"),        # utm_source
        Dimension(name="sessionMedium"),        # utm_medium
        Dimension(name="sessionCampaignName"),  # utm_campaign
    ]

    metrics = [
        Metric(name="eventCount"),
        Metric(name="ecommercePurchases"),
        Metric(name="purchaseRevenue"),
    ]

    eventos_interesse = [
        "session_start",
        "user_engagement",
        "view_item",
        "add_to_cart",
        "form_start",
        "purchase",
    ]

    request = RunReportRequest(
        property=f"properties/{property_id}",
        dimensions=dimensions,
        metrics=metrics,
        date_ranges=[DateRange(start_date="7daysAgo", end_date="yesterday")],
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
        print("GA4: nenhum dado retornado.")
        return

    colecao = db["ga4_eventos_utms"]

    # opcional: limpar dados antigos do período antes de inserir de novo
    # colecao.delete_many({"date": {"$gte": ...}})

    colecao.insert_many(linhas)
    print(f"GA4: inseridas {len(linhas)} linhas em ga4_eventos_utms.")


# --------- FUNIL (SEU CÓDIGO ADAPTADO) --------- #

def calcular_funil_ga4(db, dias=7):
    colecao = db["ga4_eventos_utms"]

    docs = list(
        colecao.find(
            {},
            {
                "_id": 0,
                "date": 1,
                "event_name": 1,
                "utm_source": 1,
                "utm_medium": 1,
                "utm_campaign": 1,
                "event_count": 1,
                "ecommerce_purchases": 1,
                "purchase_revenue": 1,
            },
        )
    )

    if not docs:
        print("Nenhum dado encontrado em ga4_eventos_utms.")
        return None

    df = pd.DataFrame(docs)

    for col in ["event_count", "ecommerce_purchases", "purchase_revenue"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    group_cols = ["utm_source", "utm_medium", "utm_campaign", "event_name"]

    agg_eventos = (
        df.groupby(group_cols, as_index=False)[
            ["event_count", "ecommerce_purchases", "purchase_revenue"]
        ]
        .sum()
    )

    tabela = agg_eventos.pivot_table(
        index=["utm_source", "utm_medium", "utm_campaign"],
        columns="event_name",
        values="event_count",
        aggfunc="sum",
        fill_value=0,
    )

    tabela = tabela.rename_axis(None, axis=1).reset_index()

    for col in ["session_start", "view_item", "add_to_cart", "form_start", "purchase"]:
        if col not in tabela.columns:
            tabela[col] = 0

    agg_ecom = (
        df.groupby(["utm_source", "utm_medium", "utm_campaign"], as_index=False)[
            ["ecommerce_purchases", "purchase_revenue"]
        ]
        .sum()
    )

    funil = tabela.merge(
        agg_ecom,
        on=["utm_source", "utm_medium", "utm_campaign"],
        how="left",
    ).fillna(0)

    funil = funil.rename(
        columns={
            "session_start": "sessoes",
            "view_item": "view_item",
            "add_to_cart": "add_to_cart",
            "form_start": "form_start",
            "purchase": "purchase_events",
        }
    )

    def safe_div(a, b):
        return (a / b).where(b > 0, 0)

    funil["taxa_view_item_por_sessao"] = safe_div(
        funil["view_item"], funil["sessoes"]
    )
    funil["taxa_add_to_cart_por_view_item"] = safe_div(
        funil["add_to_cart"], funil["view_item"]
    )
    funil["taxa_form_start_por_add_to_cart"] = safe_div(
        funil["form_start"], funil["add_to_cart"]
    )
    funil["taxa_purchase_por_form_start"] = safe_div(
        funil["ecommerce_purchases"], funil["form_start"]
    )
    funil["taxa_compra_por_sessao"] = safe_div(
        funil["ecommerce_purchases"], funil["sessoes"]
    )
    funil["ticket_medio"] = safe_div(
        funil["purchase_revenue"], funil["ecommerce_purchases"]
    )

    funil = funil.sort_values(
        by="purchase_revenue",
        ascending=False,
    ).reset_index(drop=True)

    return funil


# --------- PDF --------- #

def gerar_pdf_funil(funil: pd.DataFrame, caminho_pdf: str):
    doc = SimpleDocTemplate(caminho_pdf, pagesize=A4)
    elements = []

    styles = getSampleStyleSheet()
    titulo = Paragraph("Relatório de Funil GA4 por UTM", styles["Title"])
    data_str = datetime.utcnow().strftime("%d/%m/%Y %H:%M UTC")

    subtitulo = Paragraph(f"Gerado em: {data_str}", styles["Normal"])

    elements.append(titulo)
    elements.append(Spacer(1, 12))
    elements.append(subtitulo)
    elements.append(Spacer(1, 24))

    colunas = [
        "utm_source",
        "utm_medium",
        "utm_campaign",
        "sessoes",
        "view_item",
        "add_to_cart",
        "form_start",
        "ecommerce_purchases",
        "purchase_revenue",
        "taxa_compra_por_sessao",
        "ticket_medio",
    ]
    colunas = [c for c in colunas if c in funil.columns]

    df_pdf = funil[colunas].copy()
    df_pdf = df_pdf.head(30)

    header = [c for c in df_pdf.columns]
    data = [header] + df_pdf.values.tolist()

    table = Table(data, repeatRows=1)

    style = TableStyle(
        [
            ("BACKGROUND", (0, 0), (-1, 0), colors.grey),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
            ("ALIGN", (0, 0), (-1, -1), "CENTER"),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, 0), 9),
            ("BOTTOMPADDING", (0, 0), (-1, 0), 6),
            ("GRID", (0, 0), (-1, -1), 0.25, colors.black),
        ]
    )

    table.setStyle(style)

    elements.append(table)
    doc.build(elements)

    print(f"PDF gerado em: {caminho_pdf}")


# --------- E-MAIL --------- #

def enviar_email_com_pdf(caminho_pdf: str):
    smtp_server = os.environ["SMTP_SERVER"]
    smtp_port = int(os.environ.get("SMTP_PORT", "587"))
    smtp_user = os.environ["SMTP_USER"]
    smtp_password = os.environ["SMTP_PASSWORD"]

    email_from = os.environ.get("EMAIL_FROM", smtp_user)
    email_to = os.environ.get("EMAIL_TO", "marketing@bebidasem.com.br")

    msg = EmailMessage()
    msg["Subject"] = "Relatório de Funil GA4"
    msg["From"] = email_from
    msg["To"] = email_to
    msg.set_content(
        "Segue em anexo o relatório de funil GA4 (UTMs, sessões, eventos e compras)."
    )

    with open(caminho_pdf, "rb") as f:
        pdf_bytes = f.read()

    msg.add_attachment(
        pdf_bytes,
        maintype="application",
        subtype="pdf",
        filename=os.path.basename(caminho_pdf),
    )

    with smtplib.SMTP(smtp_server, smtp_port) as smtp:
        smtp.starttls()
        smtp.login(smtp_user, smtp_password)
        smtp.send_message(msg)

    print(f"E-mail enviado para {email_to} com o PDF em anexo.")


# --------- MAIN --------- #

def main():
    db = conectar_mongo()

    # 1) sobe base do GA4
    importar_ga4_eventos_para_mongo(db)

    # 2) calcula funil
    funil = calcular_funil_ga4(db)

    if funil is None:
        print("Nenhum dado de funil para gerar relatório.")
        return

    # 3) gera PDF
    caminho_pdf = "relatorio_funil_ga4.pdf"
    gerar_pdf_funil(funil, caminho_pdf)

    # 4) envia e-mail
    enviar_email_com_pdf(caminho_pdf)


if __name__ == "__main__":
    main()
