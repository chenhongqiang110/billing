# 使用Python 3.11运行环境作为基础镜像
FROM python:3.11-slim

# 设置工作目录
WORKDIR /app

# 将当前目录内容复制到工作目录中
COPY . /app

# 更新pip到最新版本
RUN pip install --upgrade pip

# 安装项目的依赖项
RUN pip install --no-cache-dir -r requirements.txt

# 暴露应用程序运行的端口（例如5000）
EXPOSE 8080

# 定义容器启动时运行的命令
CMD ["python", "app.py"]
