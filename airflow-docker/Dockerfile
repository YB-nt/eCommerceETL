# FROM python:3.9.17-buster
FROM apache/airflow:2.6.3-python3.10
LABEL maintainer="cordon-thiago"
# # Never prompts the user for choices on installation/configuration of packages
ENV DEBIAN_FRONTEND noninteractive
ENV TERM linux

# Airflow
ARG AIRFLOW_VERSION=2.6.3
ARG AIRFLOW_HOME=/usr/local/airflow
ARG AIRFLOW_DEPS=""
ARG PYTHON_DEPS=""
ARG SPARK_VERSION="3.1.2"
ARG HADOOP_VERSION="3.2"


ENV AIRFLOW_GPL_UNIDECODE yes
ENV AIRFLOW__WEBSERVER__AUTHENTICATE=false

ENV LANGUAGE en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8
ENV LC_CTYPE en_US.UTF-8
ENV LC_MESSAGES en_US.UTF-8

COPY requirements.txt requirements.txt
# RUN pip install --upgrade pip --no-cache-dir && \
#     pip install -r /requirements.txt --no-cache-dir && \
#     pip install airflow-docker-compose --no-cache-dir


USER root
RUN set -ex \
    && buildDeps=' \
        freetds-dev \
        libkrb5-dev \
        libsasl2-dev \
        libssl-dev \
        libffi-dev \
        libpq-dev \
        git \
    ' \
    && apt-get update -yqq \
    && apt-mark hold msodbcsql18 \  
    && apt-get upgrade -yqq \
    && apt-get install -yqq --no-install-recommends \
        $buildDeps \
        freetds-bin \
        build-essential \
        default-libmysqlclient-dev \
        apt-utils \
        curl \
        rsync \
        netcat \
        locales \
        iputils-ping \
        telnet \
    && sed -i 's/^# en_US.UTF-8 UTF-8$/en_US.UTF-8 UTF-8/g' /etc/locale.gen \
    && locale-gen \
    && update-locale LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8 
  

USER airflow

RUN pip install -U pip setuptools wheel \
    && pip install pytz \
    && pip install pyOpenSSL \
    && pip install ndg-httpsclient \
    && pip install pyasn1 \
    && pip install -r requirements.txt \
    && pip install apache-airflow[crypto,celery,postgres,hive,jdbc,mysql,ssh${AIRFLOW_DEPS:+,}${AIRFLOW_DEPS}]==${AIRFLOW_VERSION} \
    && if [ -n "${PYTHON_DEPS}" ]; then pip install ${PYTHON_DEPS}; fi 

USER root

RUN set -ex \
    && buildDeps=' \
        freetds-dev \
        libkrb5-dev \
        libsasl2-dev \
        libssl-dev \
        libffi-dev \
        libpq-dev \
        git \
    ' \
    && apt-get purge --auto-remove -yqq $buildDeps \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf \
        /var/lib/apt/lists/* \
        /tmp/* \
        /var/tmp/* \
        /usr/share/man \
        /usr/share/doc \
        /usr/share/doc-base 
        
USER airflow
RUN python --version \
&& pip freeze




# RUN set -ex \ 
#     && buildDeps=' \
#         freetds-dev \
#         libkrb5-dev \
#         libsasl2-dev \
#         libssl-dev \
#         libffi-dev \
#         libpq-dev \
#         git'\
#     && apt-get update -yqq \
#     && apt-mark hold msodbcsql18 \  
#     && apt-get upgrade -yqq \
#     && apt-get install -yqq --no-install-recommends \
#         $buildDeps \
#         freetds-bin \
#         build-essential \
#         default-libmysqlclient-dev \
#         apt-utils \
#         curl \
#         rsync \
#         netcat \
#         locales \
#         iputils-ping \
#         telnet \
#     && sed -i 's/^# en_US.UTF-8 UTF-8$/en_US.UTF-8 UTF-8/g' /etc/locale.gen \
#     && locale-gen \
#     && update-locale LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8 


    
    
# USER airflow

# RUN pip install -U pip setuptools wheel \
#     && pip install pytz \
#     && pip install pyOpenSSL \
#     && pip install ndg-httpsclient \
#     && pip install pyasn1 \
#     && pip install -r requirements.txt \
#     && pip install apache-airflow==${AIRFLOW_VERSION} \
#     && if [ -n "${PYTHON_DEPS}" ]; then pip install ${PYTHON_DEPS}; fi \
#     && apt-get purge --auto-remove -yqq $buildDeps \
#     && apt-get autoremove -yqq --purge \
#     && apt-get clean \
#     && rm -rf \
#         /var/lib/apt/lists/* \
#         /tmp/* \
#         /var/tmp/* \
#         /usr/share/man \
#         /usr/share/doc \
#         /usr/share/doc-base \
#     && python --version \
#     && pip freeze

# SPAEK
# install jdk

USER root

# RUN apt-get update && \
#     apt-get install -y software-properties-common && \
#     apt-get install -y gnupg2 && \
#     apt-key adv --keyserver keyserver.ubuntu.com --recv-keys EB9B1D8886F44E2A && \
#     add-apt-repository "deb http://security.debian.org/debian-security buster/updates main" && \ 
#     apt-get update && \
#     apt-get install -y openjdk-11-jdk && \
#     apt-get install -y python3-dbus python3-distro-info python3-gi python3-pycurl python3-software-properties software-properties-common unattended-upgrades && \
#     apt-get install -y policykit-1 libnss-systemd && \
#     java -version $$ \
#     javac -version
RUN apt-get update && \
    apt-get install -y software-properties-common && \
    apt-get install -y gnupg2 && \
    apt-key adv --keyserver keyserver.ubuntu.com --recv-keys EB9B1D8886F44E2A && \
    add-apt-repository "deb http://security.debian.org/debian-security buster/updates main" && \ 
    apt-get update && \
    mkdir -p /usr/share/man/man1 && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y ca-certificates-java && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y openjdk-11-jdk &&\
    apt-get install -y python3-dbus python3-distro-info python3-gi python3-pycurl python3-software-properties software-properties-common unattended-upgrades && \
    apt-get install -y policykit-1 libnss-systemd && \
    java -version $$ \
    javac -version
# USER airflow


ENV SCALA_HOME /usr/share/scala/
ENV SPARK_HOME /usr/local/spark/
ENV PATH $PATH:$JAVA_HOME/bin:$SCALA_HOME/bin:$SPARK_HOME/bin

# Scala와 SBT 설치
# intlij환경과 동일하게 설정
ENV SCALA_VERSION 2.12.18
ENV SBT_VERSION 1.9.2


# USER root
# SCALA 설치
RUN apt-get update && \
    apt-get install -y wget && \
    wget https://downloads.lightbend.com/scala/$SCALA_VERSION/scala-$SCALA_VERSION.deb && \
    dpkg -i scala-$SCALA_VERSION.deb && \
    rm scala-$SCALA_VERSION.deb

# SBT 설치
RUN apt-get update && \
    apt-get install -y curl && \
    curl -L -o sbt.deb https://repo.scala-sbt.org/scalasbt/debian/sbt-1.5.5.deb && \
    dpkg -i sbt.deb && \
    rm sbt.deb



ENV SPARK_VERSION 3.1.2
ENV HADOOP_VERSION 3.2

RUN wget https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz && \
    tar -xvzf spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz && \
    mv spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION $SPARK_HOME && \
    rm spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz




# COPY entrypoint.sh /entrypoint.sh
COPY airflow.cfg ${AIRFLOW_HOME}/airflow.cfg
COPY webserver_config.py ${AIRFLOW_HOME}/webserver_config.py


# RUN chown -R airflow: ${AIRFLOW_HOME}
# RUN chmod +x entrypoint.sh

EXPOSE 8080 5432 7077

USER airflow
WORKDIR ${AIRFLOW_HOME}
# ENTRYPOINT ["/entrypoint.sh"]
CMD ["webserver"]