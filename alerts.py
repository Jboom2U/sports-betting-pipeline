"""
alerts.py — Email alerting for Statalizers pipeline errors.

Sends a plain-text email when something critical breaks so you don't have
to dig through Railway logs to find out.

Setup (set these in Railway environment variables):
    ALERT_EMAIL_TO      — your email address (e.g. jskellly@gmail.com)
    ALERT_EMAIL_FROM    — Gmail address sending the alert
    ALERT_EMAIL_PASS    — Gmail App Password (NOT your regular password)
                          Generate at: myaccount.google.com/apppasswords
                          (requires 2FA enabled on the Gmail account)

If any of these are missing, alerting is silently disabled — pipeline
continues normally.

Usage:
    from alerts import send_alert
    send_alert("Odds API quota exhausted", "The Odds API returned 401. Pinnacle fallback ran instead.")
"""

import logging
import os
import smtplib
import traceback
from datetime import datetime
from email.mime.text import MIMEText
from zoneinfo import ZoneInfo

log = logging.getLogger(__name__)

ET = ZoneInfo("America/New_York")

# ── Cooldown: don't spam the same alert more than once per hour ───────────────
_last_sent: dict[str, float] = {}
COOLDOWN_SECONDS = 3600   # 1 hour between identical alerts


def _cooldown_ok(subject: str) -> bool:
    import time
    now = time.time()
    last = _last_sent.get(subject, 0)
    if now - last < COOLDOWN_SECONDS:
        return False
    _last_sent[subject] = now
    return True


def send_alert(subject: str, body: str = "", exc: Exception = None) -> bool:
    """
    Send an email alert.

    Args:
        subject: Short description of the problem (used as email subject line)
        body:    Optional longer explanation
        exc:     Optional exception — traceback is appended automatically

    Returns True if sent, False if skipped or failed.
    """
    to_addr   = os.environ.get("ALERT_EMAIL_TO", "").strip()
    from_addr = os.environ.get("ALERT_EMAIL_FROM", "").strip()
    password  = os.environ.get("ALERT_EMAIL_PASS", "").strip()

    if not all([to_addr, from_addr, password]):
        log.debug("Alert skipped — ALERT_EMAIL_* env vars not configured.")
        return False

    full_subject = f"[Statalizers] {subject}"

    if not _cooldown_ok(full_subject):
        log.debug(f"Alert suppressed (cooldown): {subject}")
        return False

    now_et    = datetime.now(ET).strftime("%Y-%m-%d %H:%M ET")
    tb_section = ""
    if exc:
        tb_section = f"\n\nTraceback:\n{traceback.format_exc()}"

    full_body = f"Time: {now_et}\n\n{body}{tb_section}\n\n— Statalizers (statalizers.com)"

    msg = MIMEText(full_body)
    msg["Subject"] = full_subject
    msg["From"]    = from_addr
    msg["To"]      = to_addr

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=10) as server:
            server.login(from_addr, password)
            server.sendmail(from_addr, [to_addr], msg.as_string())
        log.info(f"Alert sent: {subject}")
        return True
    except Exception as e:
        log.warning(f"Alert send failed (non-fatal): {e}")
        return False
