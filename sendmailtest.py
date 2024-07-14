import smtplib
from email import encoders
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


def sendMailtest():

    message = MIMEMultipart("alternative")
    message["Subject"] = "Cloud Mail"
    message["From"] = "hongqian@cn.ibm.com"
    message["To"] = "whzb@cn.ibm.com"
    message['Cc'] = "sungang@cn.ibm.com"

    text = """\
    This is an example email body.
    It can be in HTML or plain text.
    """
    part1 = MIMEText(text, "plain")
    message.attach(part1)
    attachment_paths = ['./data/user_data.xlsx']


    for attachment_path in attachment_paths:
        try:
            with open(attachment_path, "rb") as attachment:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(attachment.read())

            encoders.encode_base64(part)
            part.add_header(
                "Content-Disposition",
                f"attachment; filename= {attachment_path.split('/')[1]}",
            )
            message.attach(part)
            print("附件加载成功")
        except Exception as e:
            print(f"Failed to attach file {attachment_path}: {e}")

    smtp_server = 'smtp.outlook.com'
    port = 587

    with smtplib.SMTP(smtp_server, port) as server:
        server.starttls()
        server.login("hongqian@cn.ibm.com", "123")
        server.send_message(message)
        server.quit()
        print("Email sent successfully!")

