FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y \
    python3 python3-pip \
    mame-tools \
    p7zip-full \
    unrar \
    unzip \
    tar \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY app/ /app/

RUN pip3 install flask

VOLUME ["/source", "/destination", "/config"]
EXPOSE 9292

CMD ["python3", "app.py"]
