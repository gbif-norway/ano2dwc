FROM osgeo/gdal:ubuntu-small-3.6.3

# Update and install system and Python development dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    libpq-dev \
    python3.10 \
    python3-pip \
    python3.10-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt
COPY ./app /app
WORKDIR /app
CMD [ "tail", "-f", "/dev/null" ]
