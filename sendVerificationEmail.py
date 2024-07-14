import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


def sendMail_Verification(sender_email, receiver_email, verification_code):

    # 邮件发送者和接收者
    # sender_email = "whzb@cn.ibm.com"
    # receiver_email = "whzb@cn.ibm.com"
    # cc_email = "whzb@cn.ibm.com"

    # 创建邮件对象和设置邮件内容
    message = MIMEMultipart("alternative")
    message["Subject"] = 'Verification Code for Watson Assistant Registration'
    message["From"] = sender_email
    message["To"] = receiver_email

    # 创建邮件正文
    text = f"""\
    您注册 Watson Assistant 过程中所获得的验证码是 {verification_code}
    """
    html = f"""\
    <html>
      <body>
        <p>您注册 Watson Assistant 过程中所获得的验证码是：</p>
        <p style="font-weight: bold; font-size: 24px;">{verification_code}</p>
      </body>
    </html>
    """
    # 添加文本和HTML的部分
    part1 = MIMEText(text, "plain")
    part2 = MIMEText(html, "html")

    # 添加正文到邮件对象中
    # message.attach(part1)
    message.attach(part2)

    # 配置SMTP服务器
    smtp_server = "na.relay.ibm.com"
    # smtp_server = "emea.relay.ibm.com"
    # smtp_server = "ap.relay.ibm.com"
    # smtp_server = "la.relay.ibm.com"

    port = 25

    # 发送邮件
    try:
        # 创建SMTP会话
        server = smtplib.SMTP(smtp_server, port)
        server.starttls()  # 使用TLS加密连接

        # 发送邮件
        text = message.as_string()
        server.sendmail(sender_email, receiver_email, text)

        # 关闭SMTP会话
        server.quit()

        print("Email sent successfully!")
    except smtplib.SMTPException as e:
        print('Error: ', e)


if __name__ == '__main__':

    sender_email = "whzb@cn.ibm.com"
    receiver_email = "whzb@cn.ibm.com"
    cc_email = ["tingliu@cn.ibm.com", "hongqian@cn.ibm.com", "sungang@cn.ibm.com"]
    Subject = "test email sending"
    attachment_path = 'data/user_data.xlsx'
    verification_code = "5ZiUt8"

    sendMail_Verification(sender_email, receiver_email, verification_code)
