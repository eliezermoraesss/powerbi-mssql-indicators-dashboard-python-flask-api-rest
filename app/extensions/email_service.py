import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import logging
from app.extensions.setup_email_env import read_email_params

logging.basicConfig(level=logging.DEBUG)

email_params = read_email_params()


def send_email(subject, body):
    sender_email = email_params['sender_email']
    recipients = email_params['recipients']
    password = email_params['password']  # Use the app password generated for Gmail e-mail provider

    # Configure the email
    msg = MIMEMultipart()
    msg["From"] = sender_email
    msg["To"] = ", ".join(recipients)
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))

    # Connect to the Gmail SMTP server and send the email
    try:
        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.starttls()
        server.login(sender_email, password)

        for recipient in recipients:
            server.sendmail(sender_email, recipient, msg.as_string())
            logging.info(f"Email sent successfully to {recipient}")

        server.quit()
    except Exception as e:
        logging.error(f"Failed to send email: {e}")

