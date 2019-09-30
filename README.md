docker-airflow
===============

Fork from [docker-airflow](https://github.com/puckel/docker-airflow)

做了一些自定义修改。

## How to run

```
make
docker-compose up
```
访问localhost:8089


## 配置

```
.
├── config
│   ├── airflow.cfg              # Airflow 配置文件
│   ├── airflow.cfg.bak
│   └── requirements.txt         # 需要安装的python lib
├── dags                         # dag目录
├── docker-compose.yml           # docker启动
├── Dockerfile                   # docker镜像
├── LICENSE
├── Makefile                     # make配置
├── opt                          # 安装一些工具类库，比如jdk, datax, beeline, hadoop等， 通过挂载磁盘加载到docker容器/data/opt目录
│   ├── hosts                    # 自定义hosts
│   └── readme.md
├── plugins                      # 自定义的Airflow插件
│   ├── hooks
│   └── operators
├── README.md
├── README-old.md
├── script
│   ├── airflow_manage.sh
│   ├── bak
│   ├── cli.py                  # 修改时区为China
│   ├── entrypoint.sh           # docker容器启动脚本
│   ├── env                     # 没用，临时存储一些文件
│   ├── list_dags.sh
│   ├── master.html             # 修改时区为China
│   ├── pip.conf                # 修改python pip source
│   ├── sources.list            # 修改debian aliyun source
│   ├── sqlalchemy.py           # 修改时区为China
│   └── timezone.py             # 修改时区为China
└── update_dag.sh
```