import os
import logging
import smtplib
from email.message import EmailMessage
from typing import List, Optional, Tuple

from models import Regulation, RegulationAnalysis, ScrapeLog

logger = logging.getLogger(__name__)

def get_alert_recipients():
    raw = os.getenv("ALERT_EMAILS", "")
    emails = [e.strip() for e in raw.split(",") if e.strip()]
    return emails

def send_email(subject: str, body: str, to_emails: list[str]):
    if not to_emails:
        logger.info("Email alert skipped: no ALERT_EMAILS configured")
        return False

    host = os.getenv("SMTP_HOST")
    port = int(os.getenv("SMTP_PORT", "587"))
    user = os.getenv("SMTP_USER")
    password = os.getenv("SMTP_PASS")
    from_addr = os.getenv("SMTP_FROM", user or "compliance-monitor@localhost")
    use_tls = os.getenv("SMTP_USE_TLS", "1") == "1"

    if not host or not user or not password:
        logger.warning("Email alert skipped: SMTP credentials not fully configured")
        return False

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = from_addr
    msg["To"] = ", ".join(to_emails)
    msg.set_content(body)

    try:
        with smtplib.SMTP(host, port, timeout=30) as server:
            if use_tls:
                server.starttls()
            server.login(user, password)
            server.send_message(msg)

        logger.info("Alert email sent to %s recipients", len(to_emails))
        return True

    except Exception as e:
        logger.error("Failed to send alert email: %s", e)
        return False

def format_regulation_alert_block(reg: "Regulation", 
                                  analysis: Optional["RegulationAnalysis"]):
    lines = []
    lines.append(f"Source: {reg.source}")
    lines.append(f"Reference: {reg.reference_number}")
    lines.append(f"Title: {reg.title}")
    lines.append(f"Issue Date: {reg.issue_date}")
    lines.append(f"Document Type: {reg.document_type}")
    lines.append(f"Category: {reg.category}")
    lines.append(f"Page URL: {reg.page_url}")
    lines.append(f"Document URL: {reg.document_url}")

    if analysis and analysis.summary:
        # Use the structured part if your prompt is consistent
        gi = analysis.general_idea or ""
        impact = analysis.impact or ""
        if gi or impact:
            lines.append("\n7. General Idea:")
            lines.append(gi or "N/A")
            lines.append("\n8. Impact:")
            lines.append(impact or "N/A")
        else:
            lines.append("\nLLM Summary:")
            lines.append(analysis.summary)
    else:
        lines.append("\nLLM Summary: N/A (no analysis generated)")

    return "\n".join(str(x) for x in lines if x is not None)


def build_scrape_run_email(new_regs_with_analysis: "List[Tuple[Regulation, Optional[RegulationAnalysis]]]", log: ScrapeLog):
    total = len(new_regs_with_analysis)
    by_source = {}
    for reg, _ in new_regs_with_analysis:
        by_source[reg.source] = by_source.get(reg.source, 0) + 1

    source_str = ", ".join([f"{k}:{v}" for k, v in by_source.items()]) or "N/A"
    started = log.started_at.strftime("%Y-%m-%d %H:%M:%S UTC")

    subject = f"[Compliance Monitor] {total} new regulations ({source_str})"

    body_lines = [
        "Compliance Monitor - New Regulations Alert",
        f"Scrape started: {started}",
        f"New regulations in this run: {total}",
        f"Breakdown: {source_str}",
        "",
        "----------------------------------------",
        "",
    ]

    for i, (reg, analysis) in enumerate(new_regs_with_analysis, start=1):
        body_lines.append(f"#{i}")
        body_lines.append(format_regulation_alert_block(reg, analysis))
        body_lines.append("\n----------------------------------------\n")

    return subject, "\n".join(body_lines)

