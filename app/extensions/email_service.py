import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import logging
from app.extensions.setup_email_env import read_email_params

logging.basicConfig(level=logging.DEBUG)


def send_email(subject, body, operation=None):
    email_params = read_email_params(operation)
    sender_email = email_params['sender_email']
    password = email_params['password']  # Use the app password generated for Gmail e-mail provider

    recipients = []
    if operation is None:
        recipients = email_params['recipients']
    elif operation == 'open_late' or operation == 'open_up_to_date':
        for email_list in email_params['recipients'].values():
            for email in email_list:
                recipients.append(email)
    elif operation == 'closed_no_date':
        send_only_this_areas = ['DESENVOLVIMENTO', 'GESTAO', 'PCP', 'DIRETORIA']

        # Criar um novo dicionário contendo apenas os setores desejados
        filtered_recipients = {area: emails for area, emails in email_params['recipients'].items() if area in send_only_this_areas}

        # Extrair todos os emails do novo dicionário e armazená-los em 'recipients'
        recipients = [email for email_list in filtered_recipients.values() for email in email_list]

    recipients = list(set(recipients))
    recipients.sort()

    # Configure the email
    msg = MIMEMultipart()
    msg["From"] = sender_email
    msg["To"] = ", ".join(recipients)
    msg["Subject"] = subject

    if operation is not None:
        msg.attach(MIMEText(body, "html"))
    else:
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

