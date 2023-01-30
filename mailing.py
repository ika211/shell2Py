import datetime
import smtplib
from email.mime.text import MIMEText

from job_setup import email_addresses


def send_email(script_name, message, job_log):
    date_time = datetime.datetime.now().strftime("%m/%d/%Y %H:%M:%S")

    subject = f"{script_name} - {message} - {date_time}"

    log_content = open(job_log, "r").read()

    for address in email_addresses:
        message = MIMEText(log_content)
        message["Subject"] = subject
        message["From"] = "python_script@example.com"
        message["To"] = address
        smtp = smtplib.SMTP("localhost") # Use your SMTP server
        smtp.sendmail("python_script@example.com", [address], message.as_string())
        smtp.quit()