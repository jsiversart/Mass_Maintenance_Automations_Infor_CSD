"""
notifier.py
-----------
Lightweight Gmail-based notification utility for Python automation.
Uses Gmail App Passwords for secure SMTP login.
"""

import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from pathlib import Path
from config import GMAIL_CREDS

# === CONFIGURATION ===


def send_email(
    subject: str,
    body: str,
    to_addrs: list[str] | str = None,
    attachments: list[str] | None = None,
    html: bool = False,
    cc: list[str] | str | None = None,
    bcc: list[str] | str | None = None
):
    """
    Send an email notification through Gmail.

    Parameters
    ----------
    subject : str
        Subject line of the email.
    body : str
        Main message body.
    to_addrs : list[str] | str, optional
        One or more recipient addresses. Defaults to DEFAULT_TO.
    attachments : list[str], optional
        List of file paths to attach.
    html : bool, optional
        Set True to send HTML-formatted body.
    cc : list[str] | str, optional
        CC recipients.
    bcc : list[str] | str, optional
        BCC recipients.
    """

    # --- Normalize all recipient inputs ---
    if isinstance(to_addrs, str):
        to_addrs = [to_addrs]
    if isinstance(cc, str):
        cc = [cc]
    if isinstance(bcc, str):
        bcc = [bcc]

    to_addrs = to_addrs or GMAIL_CREDS["DEFAULT_TO"]
    cc = cc or []
    bcc = bcc or []

    # --- Build email ---
    msg = MIMEMultipart()
    msg["From"] = GMAIL_CREDS["GMAIL_USER"]
    msg["To"] = ", ".join(to_addrs)
    if cc:
        msg["Cc"] = ", ".join(cc)
    msg["Subject"] = subject

    # --- Body ---
    body_part = MIMEText(body, "html" if html else "plain")
    msg.attach(body_part)

    # --- Attachments ---
    if attachments:
        for file_path in attachments:
            file_path = Path(file_path)
            if file_path.exists():
                with open(file_path, "rb") as f:
                    part = MIMEBase("application", "octet-stream")
                    part.set_payload(f.read())
                encoders.encode_base64(part)
                part.add_header(
                    "Content-Disposition",
                    f'attachment; filename="{file_path.name}"'
                )
                msg.attach(part)
            else:
                print(f"⚠️ Attachment not found: {file_path}")

    # --- Combine recipients for sending ---
    all_recipients = to_addrs + cc + bcc

    # --- Send via Gmail ---
    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.starttls()
        server.login(GMAIL_CREDS["GMAIL_USER"], GMAIL_CREDS["GMAIL_APP_PASSWORD"])
        server.sendmail(GMAIL_CREDS["GMAIL_USER"], all_recipients, msg.as_string())

    print(f"✅ Email sent to: {', '.join(to_addrs)}"
          f"{f' | CC: {', '.join(cc)}' if cc else ''}"
          f"{f' | BCC: {', '.join(bcc)}' if bcc else ''}")
