import io
import smtplib
from datetime import datetime
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import logging
import pandas as pd
from app.extensions.setup_email_env import read_email_params

logging.basicConfig(level=logging.DEBUG)


def send_email(subject, body, operation=None, status=None, dataframe=None):
    email_params = read_email_params(status)
    sender_email = email_params['sender_email']
    password = email_params['password']  # Use the app password generated for Gmail e-mail provider
    attached_name = ''
    attached_instant_date = f'_{datetime.now().today().strftime('%d-%m-%Y_%H%M%S')}'

    recipients = []
    if status is None:
        recipients = email_params['recipients']
    elif operation == 'qp' and status == 'open_late' or status == 'open_up_to_date':
        attached_name = 'QP_ABERTA_EM_DIA' if status == 'open_up_to_date' else 'QP_ABERTA_EM_ATRASO'
        for email_list in email_params['recipients'].values():
            for email in email_list:
                recipients.append(email)
    elif operation == 'qp' and status == 'closed_no_date':
        attached_name = 'QP_FECHADA_SEM_DATA'
        send_only_this_areas = ['DESENVOLVIMENTO', 'GESTAO', 'PCP', 'DIRETORIA']
        recipients = email_extract(send_only_this_areas, email_params)
    elif operation == 'qr' and status == 'open':
        attached_name = 'QR_ABERTA'
        send_only_this_areas = ['DIRETORIA', 'DESENVOLVIMENTO', 'GESTAO', 'ALMOXARIFADO', 'PCP', 'COMPRAS', 'FISCAL',
                                'COMERCIAL']
        recipients = email_extract(send_only_this_areas, email_params)
    elif operation == 'sc' and status == 'open':
        attached_name = 'SC_ABERTA'
        send_only_this_areas = ['DIRETORIA', 'DESENVOLVIMENTO', 'GESTAO', 'ALMOXARIFADO', 'PCP', 'COMPRAS', 'FISCAL',
                                'COMERCIAL', 'ELETRICA']
        recipients = email_extract(send_only_this_areas, email_params)

    recipients = list(set(recipients))
    recipients.sort()

    # Configure the email
    msg = MIMEMultipart()
    msg["From"] = sender_email
    msg["To"] = ", ".join(recipients)
    msg["Subject"] = subject

    if status is not None:
        msg.attach(MIMEText(body, "html"))
    else:
        msg.attach(MIMEText(body, "plain"))

    if dataframe is not None:
        buffer = io.BytesIO()

        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
            dataframe.to_excel(writer, index=False, sheet_name='Data')

        buffer.seek(0)

        part = MIMEApplication(buffer.getvalue(), Name=f'{attached_name}{attached_instant_date}.xlsx')
        part['Content-Disposition'] = f'attachment; filename="{attached_name}{attached_instant_date}.xlsx"'
        msg.attach(part)

    # Connect to the Gmail SMTP server and send the email
    try:
        # server = smtplib.SMTP("mail.enaplic.com.br", 587)
        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.starttls()
        server.login(sender_email, password)

        for recipient in recipients:
            server.sendmail(sender_email, recipient, msg.as_string())
            logging.info(f"Email sent successfully to {recipient}")

        server.quit()
    except Exception as e:
        logging.error(f"Failed to send email: {e}")


def email_extract(areas: list, email_params: dict):
    # Criar um novo dicionário contendo apenas os setores desejados
    filtered_recipients = {
        area: emails for area, emails in email_params['recipients'].items() if area in areas
    }

    # Extrair todos os emails do novo dicionário e armazená-los em 'recipients'
    recipients = [email for email_list in filtered_recipients.values() for email in email_list]
    return recipients
