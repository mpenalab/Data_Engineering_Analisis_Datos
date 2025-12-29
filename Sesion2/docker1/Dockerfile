# Imagen base - referencia imagen base de python - ubuntu
FROM ubuntu:20.04

# Evitar que el SO solicite interacción manual durante la instalación
ENV DEBIAN_FRONTEND=noninteractive

# Actualizaciones de sistema e instalación de paquetes
# CORRECCIÓN: Se cambió '--y' por '-y'
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-dev \
    build-essential \
    wget && \
    apt-get clean

# Instalar jupyter
RUN pip3 install --upgrade pip && \
    pip3 install notebook==6.5.6

# Definir el directorio de trabajo
WORKDIR /workspace

# Crear el directorio para configuraciones de jupyter
# CORRECCIÓN: Se cambió '.jupiter' por '.jupyter' (con y)
RUN mkdir -p /root/.jupyter

# Configuraciones básicas
# CORRECCIÓN:
# 1. Se cambió el caracter especial '»' por el operador de redirección estándar '>>'
# 2. Se corrigió la ruta relativa a absoluta (/root/.jupyter/...)
# 3. Se corrigió el nombre de la carpeta a .jupyter
RUN echo "c.NotebookApp.token = ''" >> /root/.jupyter/jupyter_notebook_config.py && \
    echo "c.NotebookApp.password = ''" >> /root/.jupyter/jupyter_notebook_config.py && \
    echo "c.NotebookApp.open_browser = False" >> /root/.jupyter/jupyter_notebook_config.py && \
    echo "c.NotebookApp.allow_root = True" >> /root/.jupyter/jupyter_notebook_config.py

# Exponer el puerto 8888
EXPOSE 8888

# Comando por defecto
CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root"]