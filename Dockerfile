FROM python:3.11-slim

WORKDIR /app

# Dependências do sistema (openpyxl/pandas normalmente ok sem extras; mantemos minimal)
RUN pip install --no-cache-dir --upgrade pip

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY . /app

# Porta padrão do Dash
EXPOSE 8050

# BASE_SIMULADOR_PATH pode ser apontado pra um volume
# Ex: docker run -e BASE_SIMULADOR_PATH=/data/base_simulador.xlsx -v /meu/path:/data -p 8050:8050 imagem
CMD ["python", "app.py"]
