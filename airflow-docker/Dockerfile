ARG AIRFLOW_VERSION=2.7.0
FROM apache/airflow:${AIRFLOW_VERSION}-python3.9



USER root
# 필요한 패키지 설치
RUN apt-get update -yqq \
    && apt-get upgrade -yqq \
    && apt-get install -yqq --no-install-recommends \
        build-essential \
        default-libmysqlclient-dev \
        libssl-dev \
        libffi-dev \
        libxml2-dev \
        libxslt1-dev \
        libsasl2-dev

USER airflow

COPY requirements.txt .
RUN pip install --upgrade pip &&\
    pip install --no-cache-dir -r requirements.txt

# 환경 변수 설정
ENV AIRFLOW_HOME=/usr/local/airflow

HEALTHCHECK --interval=5m --timeout=3s CMD ["airflow", "version"]

USER root
RUN airflow db init && airflow users create --username airflow --password airflow --role Admin


# SPAEK
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64


ENV SCALA_HOME /usr/share/scala/
ENV SPARK_HOME /usr/local/spark/
ENV PATH $PATH:$JAVA_HOME/bin:$SCALA_HOME/bin:$SPARK_HOME/bin

# Scala와 SBT 설치
# intlij환경과 동일하게 설정
ENV SCALA_VERSION 2.12.18
ENV SBT_VERSION 1.9.2

# SCALA 설치
RUN apt-get update && \
    apt-get install -y wget && \
    wget https://downloads.lightbend.com/scala/$SCALA_VERSION/scala-$SCALA_VERSION.deb && \
    dpkg -i scala-$SCALA_VERSION.deb && \
    rm scala-$SCALA_VERSION.deb

# SBT 설치
RUN echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | tee /etc/apt/sources.list.d/sbt.list && \
    echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | tee /etc/apt/sources.list.d/sbt_old.list && \
    apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 2EE0EA64E40A89B84B2DF73499E82A75642AC823 && \
    apt-get update && \
    apt-get install -y sbt 

ENV SPARK_VERSION 3.1.2
ENV HADOOP_VERSION 3.2

RUN wget https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz && \
    tar -xvzf spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz && \
    mv spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION $SPARK_HOME && \
    rm spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz

# 패키지 제거 및 정리
USER airflow

RUN apt-get remove -y wget && \
    apt-get autoremove -y && \
    apt-get clean

EXPOSE 8080 5432 7077

CMD ["/bin/bash"]