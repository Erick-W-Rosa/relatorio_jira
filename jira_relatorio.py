# jira_relatorio.py
"""
API Flask para gerar relatório em Excel (últimos N dias) de um projeto do Jira,
enviar por e-mail e com Excel formatado (títulos PT-BR, layout moderno e
datas/hora em pt-BR). Compatível com Python 3.13.
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

# --------------------------------
# PROJETO FIXO (IDT)
# --------------------------------
PROJECT_KEY = "IDT"

# JQL fixa para o projeto IDT (NÃO usa variável de ambiente)
JIRA_JQL_TEMPLATE = (
    "project = IDT AND statusCategory = Done "
    "AND (resolutiondate >= -{days}d OR status changed to Done after -{days}d) "
    "ORDER BY resolutiondate DESC, updated DESC"
)

app = Flask(__name__)
scheduler = BackgroundScheduler(timezone=TZ)
scheduler.start()

# -----------------------------
# Helpers: TZ e Excel
# -----------------------------
def _strip_tz_inplace(df: pd.DataFrame, local_tz):
    """Remove timezone de TODAS as colunas datetime (Excel exige naive)."""
    # tz-aware dtype
    for col in df.columns:
        if pd.api.types.is_datetime64tz_dtype(df[col]):
            df[col] = df[col].dt.tz_convert(local_tz).dt.tz_localize(None)
    # objects que podem conter timestamps tz-aware
    for col in df.columns:
        if df[col].dtype == "object":
            sample = df[col].dropna().head(5)
            if sample.empty:
                continue

            def _normalize(x):
                if isinstance(x, pd.Timestamp):
                    return (x.tz_convert(local_tz).tz_localize(None) if x.tz is not None else x).to_pydatetime()
                try:
                    ts = pd.to_datetime(x, errors="raise", utc=True)
                    return ts.tz_convert(local_tz).tz_localize(None).to_pydatetime()
                except Exception:
                    return x

            df[col] = df[col].apply(_normalize)

# Ordem/colunas + traduções PT-BR
COLUMN_ORDER = [
    "key", "summary", "status", "assignee", "reporter",
    "project", "issuetype", "priority", "created", "updated", "resolved"
]
HEADER_PTBR = {
    "key": "ID",
    "summary": "Resumo",
    "status": "Status",
    "assignee": "Responsável",
    "reporter": "Autor",
    "project": "Projeto",
    "issuetype": "Tipo de Tarefa",
    "priority": "Prioridade",
    "created": "Criado em",
    "updated": "Atualizado em",
    "resolved": "Resolvido em",
}

def _apply_excel_style(writer, sheet_name: str, df_cols_ptbr: List[str]):
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    from openpyxl.utils import get_column_letter

    ws = writer.sheets[sheet_name]

    # Cabeçalho
    header_font = Font(bold=True, color="FFFFFF")
    header_fill = PatternFill(start_color="4F81BD", end_color="4F81BD", fill_type="solid")
    for c, name in enumerate(df_cols_ptbr, 1):
        cell = ws.cell(row=1, column=c)
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = Alignment(horizontal="center", vertical="center")
        ws.column_dimensions[get_column_letter(c)].width = max(18, len(name) + 2)

    # Bordas finas
    thin = Side(style="thin")
    border = Border(left=thin, right=thin, top=thin, bottom=thin)
    for row in ws.iter_rows(min_row=1, max_row=ws.max_row, min_col=1, max_col=ws.max_column):
        for cell in row:
            cell.border = border

    # Datas: dd/mm/yyyy hh:mm
    for col_name in ["Criado em", "Atualizado em", "Resolvido em"]:
        if col_name in df_cols_ptbr:
            idx = df_cols_ptbr.index(col_name) + 1
            for r in range(2, ws.max_row + 1):
                ws.cell(row=r, column=idx).number_format = "dd/mm/yyyy hh:mm"

# -----------------------------
# Jira
# -----------------------------
def fetch_tasks_from_jira(jql: str, fields: List[str] | None = None, max_results: int = 1000) -> List[Dict[str, Any]]:
    if not (JIRA_BASE_URL and JIRA_USER and JIRA_TOKEN):
        logger.warning("Jira não configurado; retornando lista vazia.")
        return []
    url = f"{JIRA_BASE_URL.rstrip('/')}/rest/api/3/search"
    headers = {"Accept": "application/json"}
    auth = (JIRA_USER, JIRA_TOKEN)

    if fields is None:
        fields = [
            "key","summary","status","assignee","reporter","project",
            "resolutiondate","created","updated","issuetype","priority"
        ]

    start_at, page_size, items = 0, 100, []
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
            items.append({
                "key": issue.get("key"),
                "summary": f.get("summary"),
                "status": (f.get("status") or {}).get("name") if f.get("status") else None,
                "assignee": (f.get("assignee") or {}).get("displayName") if f.get("assignee") else None,
                "reporter": (f.get("reporter") or {}).get("displayName") if f.get("reporter") else None,
                "project": (f.get("project") or {}).get("key") if f.get("project") else None,
                "issuetype": (f.get("issuetype") or {}).get("name") if f.get("issuetype") else None,
                "priority": (f.get("priority") or {}).get("name") if f.get("priority") else None,
                "created": f.get("created"),
                "updated": f.get("updated"),
                "resolved": f.get("resolutiondate"),
            })

        total = data.get("total", 0)
        start_at += page_size
        if start_at >= total or start_at >= max_results:
            break

    logger.info("Jira retornou %d itens", len(items))
    return items

# -----------------------------
# Relatório
# -----------------------------
def build_dataframe(items: List[Dict[str, Any]]) -> pd.DataFrame:
    df = pd.DataFrame(items)
    if df.empty:
        return pd.DataFrame(columns=COLUMN_ORDER)

    # Converter para datetime (UTC -> local tz)
    for col in ["created", "updated", "resolved"]:
        if col in df.columns:
            s = pd.to_datetime(df[col], errors="coerce", utc=True)
            s = s.dt.tz_convert(TZ)  # tz-aware
            df[col] = s

    # Remover timezone (Excel exige naive)
    _strip_tz_inplace(df, TZ)

    # Reordenar e ordenar por "resolved" (NaT vão pro fim)
    df = df[[c for c in COLUMN_ORDER if c in df.columns]]
    if "resolved" in df.columns:
        df = df.sort_values(by=["resolved"], ascending=False)
    return df

def dataframe_to_excel_bytes(df: pd.DataFrame, sheet_name: str = "Relatório IDT") -> bytes:
    df_export = df.copy().rename(columns=HEADER_PTBR)
    _strip_tz_inplace(df_export, TZ)  # última defesa
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df_export.to_excel(writer, index=False, sheet_name=sheet_name)
        _apply_excel_style(writer, sheet_name, list(df_export.columns))
    return output.getvalue()

# -----------------------------
# E-mail
# -----------------------------
def send_mail_with_attachment(subject: str, body_text: str, to_emails: List[str], attachment_name: str, attachment_bytes: bytes):
    if not to_emails:
        raise ValueError("Lista de destinatários vazia.")
    if not (SMTP_HOST and SMTP_PORT and SMTP_USER and SMTP_PASS):
        raise RuntimeError("SMTP não configurado corretamente (host/port/user/pass).")

    msg = MIMEMultipart()
    msg["From"], msg["To"], msg["Subject"] = EMAIL_FROM, ", ".join(to_emails), subject
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
def run_report_and_email(days: int = 30, jql: str | None = None, to_emails: List[str] | None = None, dry: bool = False) -> Dict[str, Any]:
    to_emails = to_emails or EMAIL_TO_DEFAULT

    # Monta JQL fixa do projeto IDT (permite override via `jql`)
    jql_final = (jql or JIRA_JQL_TEMPLATE.format(days=days))

    items = fetch_tasks_from_jira(jql_final) if jql_final else []

    # Filtro defensivo: 1) tenta resolved; 2) fallback para updated
    if items:
        cutoff = datetime.now(tz=TZ) - timedelta(days=days)
        keep = []
        for it in items:
            resolved_dt = pd.to_datetime(it.get("resolved"), errors="coerce", utc=True)
            if pd.notna(resolved_dt):
                resolved_dt = resolved_dt.tz_convert(TZ)
                if resolved_dt >= cutoff:
                    keep.append(it)
                continue
            updated_dt = pd.to_datetime(it.get("updated"), errors="coerce", utc=True)
            if pd.notna(updated_dt):
                updated_dt = updated_dt.tz_convert(TZ)
                if updated_dt >= cutoff:
                    keep.append(it)
        items = keep

    df = build_dataframe(items)
    excel_bytes = dataframe_to_excel_bytes(df)

    now = datetime.now(tz=TZ)
    filename = f"relatorio_{PROJECT_KEY.lower()}_{now.strftime('%Y%m%d_%H%M')}.xlsx"

    if dry:
        return {"ok": True, "dry": True, "rows": int(len(df.index)), "filename": filename}

    subject = f"Relatório {PROJECT_KEY} - tarefas concluídas (últimos {days} dias) - {now.strftime('%d/%m/%Y %H:%M')}"
    body = (
        f"Segue em anexo o relatório em Excel do projeto {PROJECT_KEY} com tarefas concluídas "
        f"nos últimos {days} dias.\nGerado em {now.strftime('%d/%m/%Y %H:%M %Z')}."
    )
    send_mail_with_attachment(subject, body, to_emails, filename, excel_bytes)

    return {
        "ok": True,
        "rows": int(len(df.index)),
        "filename": filename,
        "sent_to": to_emails,
        "generated_at": now.isoformat(),
    }

# -----------------------------
# API
# -----------------------------
@app.get("/health")
def health():
    return jsonify({"status": "ok", "time": datetime.now(tz=TZ).isoformat(), "project": PROJECT_KEY})

@app.post("/report/run")
def report_run():
    data = request.get_json(silent=True) or {}
    days = int(data.get("days", 30))
    jql = data.get("jql")  # override por requisição, se quiser
    to_emails = data.get("to")
    if isinstance(to_emails, str):
        to_emails = [e.strip() for e in to_emails.split(",") if e.strip()]
    dry = str(data.get("dry", "")).lower() in {"1", "true", "yes"}

    try:
        result = run_report_and_email(days=days, jql=jql, to_emails=to_emails, dry=dry)
        return jsonify(result)
    except Exception as e:
        logger.exception("Erro executando relatório: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 500

@app.post("/schedule/monthly")
def schedule_monthly():
    data = request.get_json(force=True)
    day = int(data.get("day", 1))
    time_str = data.get("time", "09:00")
    days_window = int(data.get("days", 30))
    jql = data.get("jql")
    to_emails = data.get("to") or EMAIL_TO_DEFAULT
    if isinstance(to_emails, str):
        to_emails = [e.strip() for e in to_emails.split(",") if e.strip()]
    h, m = map(int, time_str.split(":"))
    job_id = data.get("job_id", f"monthly_{day:02d}_{h:02d}{m:02d}")
    trigger = CronTrigger(day=day, hour=h, minute=m, timezone=TZ)

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
    time_str = data.get("time", "09:00")
    days_window = int(data.get("days", 30))
    jql = data.get("jql")
    to_emails = data.get("to") or EMAIL_TO_DEFAULT
    if isinstance(to_emails, str):
        to_emails = [e.strip() for e in to_emails.split(",") if e.strip()]
    h, m = map(int, time_str.split(":"))
    job_id = data.get("job_id", f"daily_{h:02d}{m:02d}")
    trigger = CronTrigger(hour=h, minute=m, timezone=TZ)

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
        jobs.append({
            "id": job.id,
            "next_run_time": job.next_run_time.astimezone(TZ).isoformat() if job.next_run_time else None,
            "trigger": str(job.trigger),
        })
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
    logger.info("Iniciando API na porta %d (TZ=%s, PROJECT=%s)...", port, TZ, PROJECT_KEY)
    app.run(host="0.0.0.0", port=port)
