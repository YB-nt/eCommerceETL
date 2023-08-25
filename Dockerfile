ENV AIRFLOW__CORE__EXECUTOR = LocalExecutor

ENV _AIRFLOW_DB_UPGRADE=true
ENV _AIRFLOW_WWW_USER_CREATE=true
ENV _AIRFLOW_WWW_USER_USERNAME = ${AIRFLOWID}
ENV _AIRFLOW_WWW_USER_PASSWORD = ${AIRFLOWPASSWORD}

RUN airflow db init
HEALTHCHECK --interval=5m --timeout=3s CMD ["airflow", "version"]

ARG AIRFLOW_VERSION=2.6.3
ARG AIRFLOW_HOME=/usr/local/airflow
ARG AIRFLOW_DEPS=""
ARG PYTHON_DEPS=""
ARG SPARK_VERSION="3.0.1"
ARG HADOOP_VERSION="2.7"
ENV AIRFLOW_GPL_UNIDECODE yes

# python install requirements

# COPY ./dag/requirements.txt /requirements.txt
# RUN pip install -r requirements.txt

# COPY ./dags/requirements.txt /opt/airflow/requirements.txt
RUN python -m pip install --upgrade pip &&\
    pip install -r /dags/requirements.txt

# SPAEK

COPY ./env ./spark/.env

USER root
RUN apt-get update && \
    apt-get install -y openjdk-8-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64
USER airflow

# Scala와 SBT 설치
# intlij환경과 동일하게 설정
ENV SCALA_VERSION 2.12.18
ENV SBT_VERSION 1.9.2

RUN apt-get update && \
    apt-get install -y wget && \
    wget https://downloads.lightbend.com/scala/$SCALA_VERSION/scala-$SCALA_VERSION.deb && \
    dpkg -i scala-$SCALA_VERSION.deb && \
    wget https://dl.bintray.com/sbt/debian/sbt-$SBT_VERSION.deb && \
    dpkg -i sbt-$SBT_VERSION.deb && \
    rm scala-$SCALA_VERSION.deb sbt-$SBT_VERSION.deb && \
    apt-get remove -y wget && \
    apt-get autoremove -y && \
    apt-get clean

ENV SPARK_VERSION 3.1.2
ENV HADOOP_VERSION 3.2

RUN wget https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz && \
    tar -xvzf spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz && \
    mv spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION /usr/local/spark && \
    rm spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz

# 애플리케이션 코드 추가
COPY . /app/
WORKDIR /app
RUN sbt package

# Master와 Worker 설정
COPY master.sh /master.sh
COPY worker.sh /worker.sh
COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf

# 환경변수 구성
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/
ENV SCALA_HOME /usr/share/scala/
ENV SPARK_HOME /usr/local/spark/
ENV PATH $PATH:$JAVA_HOME/bin:$SCALA_HOME/bin:$SPARK_HOME/bin


EXPOSE 8080 7077 6066

CMD ["/bin/bash"]