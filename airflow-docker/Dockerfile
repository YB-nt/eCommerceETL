ARG AIRFLOW_HOME=/usr/local/airflow
ARG AIRFLOW_DEPS=""
ARG PYTHON_DEPS=""
ARG SPARK_VERSION="3.0.1"
ARG HADOOP_VERSION="2.7"
ENV AIRFLOW_GPL_UNIDECODE yes

ENV AIRFLOW__CORE__EXECUTOR = LocalExecutor

ENV _AIRFLOW_DB_UPGRADE=true
ENV _AIRFLOW_WWW_USER_CREATE=true
ENV _AIRFLOW_WWW_USER_USERNAME = ${AIRFLOWID}
ENV _AIRFLOW_WWW_USER_PASSWORD = ${AIRFLOWPASSWORD}

FROM apache/airflow:latest-python3.9
ADD requirements.txt . 
RUN python -m pip install --upgrade pip &&\
    pip install -r requirements.txt

RUN airflow db init
HEALTHCHECK --interval=5m --timeout=3s CMD ["airflow", "version"]



# python install requirements

# COPY ./dag/requirements.txt /requirements.txt
# RUN pip install -r requirements.txt

# COPY ./dags/requirements.txt /opt/airflow/requirements.txt
# RUN python -m pip install --upgrade pip &&\
#     pip install -r /dags/requirements.txt

# SPAEK
USER root
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

RUN apt-get install build-dep python-psycopg2


RUN apt-get remove -y wget && \
    apt-get autoremove -y && \
    apt-get clean

# # 애플리케이션 코드 추가
# COPY ../spark/ /app/
# WORKDIR /app
# RUN sbt package

# # Master와 Worker 설정
# COPY master.sh /master.sh
# COPY worker.sh /worker.sh
# COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf



EXPOSE 8080 7077 6066

CMD ["/bin/bash"]