# jira_relatorio.py
"""
API Flask para gerar relatório em Excel com tarefas concluídas nos últimos N dias
e enviar por e-mail. Integração opcional com Jira (via JQL).
Compatível com Python 3.13.
"""

from __future__ import annotations

import io
import os
import smtplib
import logging
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email import encoders
from datetime import datetime, timedelta
from typing import List, Dict, Any

import pandas as pd
import requests
from dateutil import tz
from flask import Flask, jsonify, request
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

# -----------------------------
# Configuração e Logger
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("jira_relatorio")

TZ = tz.gettz(os.getenv("APP_TZ", "America/Sao_Paulo"))

SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASS = os.getenv("SMTP_PASS")
EMAIL_FROM = os.getenv("EMAIL_FROM", SMTP_USER or "noreply@example.com")
EMAIL_TO_DEFAULT = [e.strip() for e in os.getenv("EMAIL_TO", "").split(",") if e.strip()]

JIRA_BASE_URL = os.getenv("JIRA_BASE_URL")
JIRA_USER = os.getenv("JIRA_USER")
JIRA_TOKEN = os.getenv("JIRA_TOKEN")
# ⚠️ JQL como STRING (corrigido)
JIRA_JQL_DEFAULT = os.getenv(
    "JIRA_JQL",
    "statusCategory = Done AND resolved >= -30d ORDER BY resolved DESC",
)

app = Flask(__name__)
scheduler = BackgroundScheduler(timezone=TZ)
scheduler.start()

# -----------------------------
# Data Source: Jira (opcional)
# -----------------------------
def fetch_tasks_from_jira(
    jql: str,
    fields: List[str] | None = None,
    max_results: int = 1000,
) -> List[Dict[str, Any]]:
    """
    Busca issues do Jira via REST usando a JQL informada.
    Retorna uma lista de dicionários simples, prontos para DataFrame.
    """
    if not (JIRA_BASE_URL and JIRA_USER and JIRA_TOKEN):
        logger.warning("Jira não configurado; retornando lista vazia.")
        return []

    url = f"{JIRA_BASE_URL.rstrip('/')}/rest/api/3/search"
    headers = {"Accept": "application/json"}
    auth = (JIRA_USER, JIRA_TOKEN)

    if fields is None:
        fields = [
            "key",
            "summary",
            "status",
            "assignee",
            "reporter",
            "project",
            "resolutiondate",
            "created",
            "updated",
            "issuetype",
            "priority",
        ]

    start_at = 0
    page_size = 100
    items: List[Dict[str, Any]] = []

    while start_at < max_results:
        payload = {
            "jql": jql,
            "startAt": start_at,
            "maxResults": min(page_size, max_results - start_at),
            "fields": fields,
        }
        resp = requests.post(url, json=payload, headers=headers, auth=auth, timeout=60)
        resp.raise_for_status()
        data = resp.json()

        for issue in data.get("issues", []):
            f = issue.get("fields", {})
            items.append(
                {
                    "key": issue.get("key"),
                    "summary": f.get("summary"),
                    "status": (f.get("status") or {}).get("name") if f.get("status") else None,
                    "assignee": ((f.get("assignee") or {}).get("displayName")) if f.get("assignee") else None,
                    "reporter": ((f.get("reporter") or {}).get("displayName")) if f.get("reporter") else None,
                    "project": ((f.get("project") or {}).get("key")) if f.get("project") else None,
                    "issuetype": ((f.get("issuetype") or {}).get("name")) if f.get("issuetype") else None,
                    "priority": ((f.get("priority") or {}).get("name")) if f.get("priority") else None,
                    "created": f.get("created"),
                    "updated": f.get("updated"),
                    "resolved": f.get("resolutiondate"),
                }
            )

        total = data.get("total", 0)
        start_at += page_size
        if start_at >= total or start_at >= max_results:
            break

    logger.info("Jira retornou %d itens", len(items))
    return items


# -----------------------------
# Geração de Relatório
# -----------------------------
def build_dataframe(items: List[Dict[str, Any]]) -> pd.DataFrame:
    df = pd.DataFrame(items)
    if df.empty:
        return df

    # Conversões de data
    for col in ["created", "updated", "resolved"]:
        if col in df.columns:
            series = pd.to_datetime(df[col], errors="coerce", utc=True)
            df[col] = series.dt.tz_convert(TZ)

    # Ordenar por resolved desc, se existir
    if "resolved" in df.columns:
        df = df.sort_values(by=["resolved"], ascending=False)

    return df


def dataframe_to_excel_bytes(df: pd.DataFrame, sheet_name: str = "Concluidas") -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        if df.empty:
            pd.DataFrame(
                columns=[
                    "key",
                    "summary",
                    "status",
                    "assignee",
                    "reporter",
                    "project",
                    "issuetype",
                    "priority",
                    "created",
                    "updated",
                    "resolved",
                ]
            ).to_excel(writer, index=False, sheet_name=sheet_name)
        else:
            df.to_excel(writer, index=False, sheet_name=sheet_name)
    return output.getvalue()


# -----------------------------
# Envio de E-mail
# -----------------------------
def send_mail_with_attachment(
    subject: str,
    body_text: str,
    to_emails: List[str],
    attachment_name: str,
    attachment_bytes: bytes,
):
    if not to_emails:
        raise ValueError("Lista de destinatários vazia.")
    if not (SMTP_HOST and SMTP_PORT and SMTP_USER and SMTP_PASS):
        raise RuntimeError("SMTP não configurado corretamente (host/port/user/pass).")

    msg = MIMEMultipart()
    msg["From"] = EMAIL_FROM
    msg["To"] = ", ".join(to_emails)
    msg["Subject"] = subject
    msg.attach(MIMEText(body_text, "plain", "utf-8"))

    part = MIMEBase("application", "octet-stream")
    part.set_payload(attachment_bytes)
    encoders.encode_base64(part)
    part.add_header("Content-Disposition", f"attachment; filename={attachment_name}")
    msg.attach(part)

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        server.starttls()
        server.login(SMTP_USER, SMTP_PASS)
        server.sendmail(EMAIL_FROM, to_emails, msg.as_string())

    logger.info("E-mail enviado para: %s", to_emails)


# -----------------------------
# Orquestração
# -----------------------------
def run_report_and_email(
    days: int = 30,
    jql: str | None = None,
    to_emails: List[str] | None = None,
) -> Dict[str, Any]:
    """
    Executa o fluxo completo: busca dados (Jira, se configurado), filtra janela de 'days',
    gera Excel, envia por e-mail. Retorna metadados da execução.
    """
    to_emails = to_emails or EMAIL_TO_DEFAULT

    # Monta JQL padrão caso necessário
    jql = jql or (JIRA_JQL_DEFAULT.replace("-30d", f"-{days}d") if JIRA_JQL_DEFAULT else None)

    items: List[Dict[str, Any]] = fetch_tasks_from_jira(jql) if jql else []

    # Filtro defensivo por janela de dias, caso a fonte não aplique
    if items:
        cutoff = datetime.now(tz=TZ) - timedelta(days=days)
        norm_items: List[Dict[str, Any]] = []
        for it in items:
            resolved_str = it.get("resolved")
            resolved_dt = pd.to_datetime(resolved_str, errors="coerce", utc=True) if resolved_str else pd.NaT
            if pd.notna(resolved_dt) and resolved_dt.tzinfo is not None:
                resolved_dt = resolved_dt.tz_convert(TZ)
            if pd.notna(resolved_dt) and resolved_dt >= cutoff:
                norm_items.append(it)
        items = norm_items

    df = build_dataframe(items)
    excel_bytes = dataframe_to_excel_bytes(df)

    now = datetime.now(tz=TZ)
    filename = f"relatorio_tarefas_concluidas_{now.strftime('%Y%m%d_%H%M')}.xlsx"
    subject = f"Relatório de tarefas concluídas (últimos {days} dias) - {now.strftime('%d/%m/%Y %H:%M')}"
    body = (
        f"Segue em anexo o relatório em Excel com tarefas concluídas nos últimos {days} dias.\n"
        f"Gerado em {now.strftime('%d/%m/%Y %H:%M %Z')}."
    )

    send_mail_with_attachment(subject, body, to_emails, filename, excel_bytes)

    return {
        "rows": len(df.index),
        "filename": filename,
        "sent_to": to_emails,
        "generated_at": now.isoformat(),
    }


# -----------------------------
# API
# -----------------------------
@app.get("/health")
def health():
    return jsonify({"status": "ok", "time": datetime.now(tz=TZ).isoformat()})


@app.post("/report/run")
def report_run():
    data = request.get_json(silent=True) or {}
    days = int(data.get("days", 30))
    jql = data.get("jql")
    to_emails = data.get("to")
    if isinstance(to_emails, str):
        to_emails = [e.strip() for e in to_emails.split(",") if e.strip()]
    try:
        result = run_report_and_email(days=days, jql=jql, to_emails=to_emails)
        return jsonify({"ok": True, **result})
    except Exception as e:
        logger.exception("Erro executando relatório: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 500


@app.post("/schedule/monthly")
def schedule_monthly():
    data = request.get_json(force=True)
    # Ex.: {"day": 1, "time": "08:30", "days": 30, "jql": "...", "to": ["a@b.com"]}
    day = int(data.get("day", 1))  # 1-28 recomendado
    time_str = data.get("time", "09:00")
    days_window = int(data.get("days", 30))
    jql = data.get("jql")
    to_emails = data.get("to") or EMAIL_TO_DEFAULT
    if isinstance(to_emails, str):
        to_emails = [e.strip() for e in to_emails.split(",") if e.strip()]

    hour, minute = map(int, time_str.split(":"))
    job_id = data.get("job_id", f"monthly_{day:02d}_{hour:02d}{minute:02d}")

    trigger = CronTrigger(day=day, hour=hour, minute=minute, timezone=TZ)

    def job():
        logger.info("[JOB %s] Executando relatório mensal...", job_id)
        run_report_and_email(days=days_window, jql=jql, to_emails=to_emails)

    if scheduler.get_job(job_id):
        scheduler.remove_job(job_id)

    scheduler.add_job(job, trigger=trigger, id=job_id, replace_existing=True)
    return jsonify({"ok": True, "job_id": job_id, "when": str(trigger)})


@app.post("/schedule/daily")
def schedule_daily():
    data = request.get_json(force=True)
    # Ex.: {"time": "08:30", "days": 30, "jql": "...", "to": ["a@b.com"], "job_id": "diario_0830"}
    time_str = data.get("time", "09:00")
    days_window = int(data.get("days", 30))
    jql = data.get("jql")
    to_emails = data.get("to") or EMAIL_TO_DEFAULT
    if isinstance(to_emails, str):
        to_emails = [e.strip() for e in to_emails.split(",") if e.strip()]

    hour, minute = map(int, time_str.split(":"))
    job_id = data.get("job_id", f"daily_{hour:02d}{minute:02d}")

    trigger = CronTrigger(hour=hour, minute=minute, timezone=TZ)

    def job():
        logger.info("[JOB %s] Executando relatório diário...", job_id)
        run_report_and_email(days=days_window, jql=jql, to_emails=to_emails)

    if scheduler.get_job(job_id):
        scheduler.remove_job(job_id)

    scheduler.add_job(job, trigger=trigger, id=job_id, replace_existing=True)
    return jsonify({"ok": True, "job_id": job_id, "when": str(trigger)})


@app.get("/schedule")
def schedule_list():
    jobs = []
    for job in scheduler.get_jobs():
        jobs.append(
            {
                "id": job.id,
                "next_run_time": job.next_run_time.astimezone(TZ).isoformat() if job.next_run_time else None,
                "trigger": str(job.trigger),
            }
        )
    return jsonify({"ok": True, "jobs": jobs})


@app.delete("/schedule/<job_id>")
def schedule_delete(job_id: str):
    job = scheduler.get_job(job_id)
    if not job:
        return jsonify({"ok": False, "error": "job_id não encontrado"}), 404
    scheduler.remove_job(job_id)
    return jsonify({"ok": True, "removed": job_id})


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    logger.info("Iniciando API na porta %d (TZ=%s)...", port, TZ)
    app.run(host="0.0.0.0", port=port)
