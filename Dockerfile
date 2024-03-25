FROM python:3.10-slim-bullseye

RUN apt update

WORKDIR /app
COPY . /app

RUN apt-get update && apt-get install -y \
    curl \
    tar \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

RUN curl -SLO http://downloads.sourceforge.net/project/ta-lib/ta-lib/0.4.0/ta-lib-0.4.0-src.tar.gz \
    && tar -xzf ta-lib-0.4.0-src.tar.gz \
    && rm ta-lib-0.4.0-src.tar.gz

RUN cd ta-lib \
    && ./configure --prefix=/usr \
    && make \
    && make install \
    && cd .. \
    && rm -rf ta-lib

RUN pip install --no-cache-dir -r requirements.txt

CMD ["/app/main.py"]
ENTRYPOINT ["python"]