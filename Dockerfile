# VERSION 1.10.5
# AUTHOR: Ryan Miao
# DESCRIPTION: Basic Airflow container
# BUILD: docker build --rm -t puckel/docker-airflow .
# SOURCE: https://github.com/puckel/docker-airflow

FROM python:3.7-slim-stretch
LABEL maintainer="Ryan Miao"

# Never prompts the user for choices on installation/configuration of packages
ENV DEBIAN_FRONTEND noninteractive
ENV TERM linux

# Airflow
ARG AIRFLOW_VERSION=1.10.5
ARG AIRFLOW_USER_HOME=/usr/local/airflow
ARG AIRFLOW_USER=airflow
ARG AIRFLOW_DEPS=""
ARG PYTHON_DEPS=""
ENV AIRFLOW_HOME=${AIRFLOW_USER_HOME}

# Define en_US.
ENV LANGUAGE zh_CN.UTF-8
ENV LANG zh_CN.UTF-8
ENV LC_ALL zh_CN.UTF-8
ENV LC_CTYPE zh_CN.UTF-8
ENV LC_MESSAGES zh_CN.UTF-8


COPY script/sources.list  /etc/apt/sources.list


RUN set -ex \
    && buildDeps=' \
        freetds-dev \
        libkrb5-dev \
        libsasl2-dev \
        libssl-dev \
        libffi-dev \
        libpq-dev \
    ' \
    && apt-get update -yqq \
    && apt-get upgrade -yqq \
    && apt-get install -yqq --no-install-recommends \
        $buildDeps \
        freetds-bin \
        build-essential \
        default-libmysqlclient-dev \
        apt-utils \
        curl \
        rsync \
        git \
        netcat \
        locales \
    && sed -i 's/^# zh_CN.UTF-8 UTF-8$/zh_CN.UTF-8 UTF-8/g' /etc/locale.gen \
    && locale-gen \
    && update-locale LANG=zh_CN.UTF-8 LC_ALL=zh_CN.UTF-8 \
    && /bin/cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime && echo 'Asia/Shanghai' >/etc/timezone \
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

RUN  useradd -ms /bin/bash -d ${AIRFLOW_USER_HOME} -p "$(openssl passwd -1 airflow)"  ${AIRFLOW_USER} \
     && chown -R ${AIRFLOW_USER}: ${AIRFLOW_USER_HOME}
RUN  mkdir /root/.pip && mkdir ${AIRFLOW_USER_HOME}/.pip
COPY script/pip.conf  /root/.pip/pip.conf
COPY script/pip.conf  ${AIRFLOW_USER_HOME}/.pip/pip.conf
RUN  pip install -i http://mirrors.aliyun.com/pypi/simple  -U pip setuptools wheel \
    && pip install -i http://mirrors.aliyun.com/pypi/simple  pytz pyOpenSSL \
    ndg-httpsclient \
    pyasn1 \
    apache-airflow[crypto,celery,postgres,hive,jdbc,mysql,ssh${AIRFLOW_DEPS:+,}${AIRFLOW_DEPS}]==${AIRFLOW_VERSION} \
   'redis==3.2' \
    ldap3 \
    && if [ -n "${PYTHON_DEPS}" ]; then pip install -i http://mirrors.aliyun.com/pypi/simple  ${PYTHON_DEPS}; fi \
    && rm -rf \
        /tmp/* \
        /var/tmp/* 

COPY script/entrypoint.sh /entrypoint.sh
COPY script/airflow_manage.sh /airflow_manage.sh
COPY config/airflow.cfg ${AIRFLOW_USER_HOME}/airflow.cfg
COPY script/cli.py /usr/local/lib/python3.7/site-packages/airflow/bin/cli.py
COPY script/timezone.py /usr/local/lib/python3.7/site-packages/airflow/utils/timezone.py
COPY script/sqlalchemy.py /usr/local/lib/python3.7/site-packages/airflow/utils/sqlalchemy.py
COPY script/master.html /usr/local/lib/python3.7/site-packages/airflow/www/templates/admin/master.html



EXPOSE 8080 5555 8793

# ENV DATAX_HOME=/data/datax
# ENV JAVA_HOME=/data/opt/jdk1.8.0_221
# ENV HADOOP_HOME=/data/opt/hadoop-2.7.7
# ENV HIVE_HOME=/data/opt/apache-hive-1.1.0-bin
# ENV PATH="${JAVA_HOME}/bin:${HADOOP_HOME}/bin:${HIVE_HOME}/bin:${PATH}"

COPY script/list_dags.sh /list_dags.sh

RUN chmod 777 /etc/profile.d && chmod 777 /etc/hosts
#USER ${AIRFLOW_USER}
WORKDIR ${AIRFLOW_USER_HOME}
ENTRYPOINT ["/entrypoint.sh"]
CMD ["webserver"] # set default arg for entrypoint
