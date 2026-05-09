FROM ubuntu:20.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get upgrade -y && apt-get install -y \
    python3-pip \
    openssh-server \
    iputils-ping \
    curl \
    netcat \
    bash \
    vim \
    less \
    openjdk-11-jdk \
    && rm -rf /var/lib/apt/lists/*


ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH


RUN mkdir /var/run/sshd && \
    echo 'root:root' | chpasswd && \
    sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config && \
    sed -i 's/#PasswordAuthentication yes/PasswordAuthentication yes/' /etc/ssh/sshd_config

EXPOSE 22
EXPOSE 4040

COPY requirements.txt /app/
WORKDIR /app

RUN pip3 install -r requirements.txt

# download PostgreSQL JDBC driver into pyspark jars directory
RUN curl -o /usr/local/lib/python3.8/dist-packages/pyspark/jars/postgresql-42.6.0.jar \
    https://jdbc.postgresql.org/download/postgresql-42.6.0.jar


COPY . /app/

RUN chmod +x /app/entrypoint.sh

CMD ["/app/entrypoint.sh"]
