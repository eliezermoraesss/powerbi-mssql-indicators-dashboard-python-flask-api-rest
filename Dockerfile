# Usar a imagem base oficial do Python
FROM python:latest

# Definir o diretório de trabalho no contêiner
WORKDIR /usr/src/app

# Copiar os arquivos de requisitos e instalar as dependências
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copiar o restante dos arquivos da aplicação
COPY . .

# Expor a porta que o Flask usará
EXPOSE 5000

# Comando para rodar a aplicação
CMD ["python", "app.py"]

