import smtplib
import os
from email.message import EmailMessage
from dotenv import load_dotenv

load_dotenv()

def send_email_with_attachment(subject, body, filepath, filename="report.pdf"):
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = os.getenv("EMAIL_USERNAME")
    msg["To"] = os.getenv("EMAIL_RECEIVER")
    msg.set_content(body)

    # Voeg de PDF toe als bijlage
    with open(filepath, "rb") as f:
        file_data = f.read()
        msg.add_attachment(file_data, maintype="application", subtype="pdf", filename=filename)

    with smtplib.SMTP(os.getenv("EMAIL_HOST"), int(os.getenv("EMAIL_PORT"))) as smtp:
        smtp.starttls()
        smtp.login(os.getenv("EMAIL_USERNAME"), os.getenv("EMAIL_PASSWORD"))
        smtp.send_message(msg)
