FROM apache/airflow:2.7.2-python3.11

COPY requirements.txt .

RUN pip install -r requirements.txt

USER root

RUN sed -i 's|http://deb.debian.org|https://deb.debian.org|g; s|http://security.debian.org|https://security.debian.org|g' /etc/apt/sources.list && \
    mkdir -p /var/lib/apt/lists/partial && \
    chmod -R 755 /var/lib/apt/lists && \
    apt-get update -o Acquire::Retries=10 && \
    apt-get install -y --no-install-recommends openjdk-11-jdk nano && \
    apt-get -f install -y && \
    rm -rf /var/lib/apt/lists/*


ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/

#RUN mkdir -p /opt/spark
#COPY spark-3.5.0-bin-hadoop3.tgz /opt/spark
#RUN tar -xvzf /opt/spark/spark-3.5.0-bin-hadoop3.tgz --strip-components=1 -C /opt/spark
#RUN rm /opt/spark/spark-3.5.0-bin-hadoop3.tgz
#ENV SPARK_HOME /opt/spark
#RUN export SPARK_HOME
#RUN export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

RUN mkdir /shared && chown -R airflow /shared

USER airflow

WORKDIR /opt/airflow
