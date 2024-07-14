import smtplib
from email import encoders
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


def sendMail_Report(sender_email, subject, receiver_email, cc_email, attachment_paths):
    # 邮件发送者和接收者
    # sender_email = "whzb@cn.ibm.com"
    # receiver_email = "whzb@cn.ibm.com"
    # cc_email = "whzb@cn.ibm.com"

    # 创建邮件对象和设置邮件内容
    message = MIMEMultipart("alternative")
    message["Subject"] = subject
    message["From"] = sender_email
    # message["To"] = receiver_email
    message["To"] = ';'.join(receiver_email)
    message['Cc'] = ';'.join(cc_email)

    # 创建邮件正文
    text = """\
    This is an example email body.
    It can be in HTML or plain text.
    """
    html = """\
    <html>
      <body>
        <p>This is Billing Report.</p>
      </body>
    </html>
    """
    # 添加文本和HTML的部分
    part1 = MIMEText(text, "plain")
    part2 = MIMEText(html, "html")

    # 添加正文到邮件对象中
    message.attach(part1)
    message.attach(part2)

    # Attach the file
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

    # 配置SMTP服务器
    # smtp_server = "na.relay.ibm.com"
    smtp_server = "emea.relay.ibm.com"
    port = 25

    # 发送邮件
    try:
        # 创建SMTP会话
        server = smtplib.SMTP(smtp_server, port)
        server.starttls()  # 使用TLS加密连接

        # 发送邮件
        text = message.as_string()

        # to_addrs = [receiver_email]
        # if ',' in cc_email:
        #     to_addrs += cc_email.split(',')
        # else:
        #     to_addrs.append(cc_email)
        #
        # print("to_addrs:", to_addrs)
        #
        # # server.sendmail(sender_email, to_addrs, text)
        # server.sendmail(sender_email, receiver_email + cc_email, text)

        server.sendmail(sender_email, receiver_email, text)

        # 关闭SMTP会话
        server.quit()

        print("Email sent successfully!")
    except smtplib.SMTPException as e:
        print('Error: ', e)


if __name__ == '__main__':
    sender_email = "whzb@cn.ibm.com"
    receiver_email1 = ['tingliu@cn.ibm.com', 'hongqian@cn.ibm.com', 'sungang@cn.ibm.com', 'whzb@cn.ibm.com']
    cc_email1 = ['tingliu@cn.ibm.com', 'hongqian@cn.ibm.com', 'sungang@cn.ibm.com']
    receiver_email = ['whzb@cn.ibm.com']
    cc_email = ['hongqian@cn.ibm.com']
    Subject = "邮件发送测试！！！！！！"
    # attachment_path = 'data/user_data.xlsx'
    attachment_path = ''

    sendMail_Report(sender_email, Subject, receiver_email1, cc_email, attachment_path)
