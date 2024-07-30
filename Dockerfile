# Usar uma imagem base oficial do Python
FROM python:3.9-slim

# Definir o diretório de trabalho no contêiner
WORKDIR /app

# Copiar o arquivo de requisitos e instalar dependências
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copiar o conteúdo do diretório atual para o diretório de trabalho no contêiner
COPY . .

# Definir a variável de ambiente para indicar a execução do Flask em produção
ENV FLASK_ENV=production

# Expor a porta que a aplicação Flask irá rodar
EXPOSE 5000

# Comando para rodar a aplicação usando Gunicorn
CMD ["gunicorn", "--bind", "0.0.0.0:5000", "app.views.indicator_views:app"]
