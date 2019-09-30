# -*- coding: utf-8 -*-
#


"""
datax入库hive
"""
import subprocess
import uuid
import json
import os

from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook


class RDBMS2HiveHook(BaseHook):
    """
    Datax执行器 
    """

    def __init__(self, 
                 task_id,
                 conn_id,
                 query_sql,
                 hive_db,
                 hive_table,
                 hive_table_column,
                 hive_table_partition,
                 split_pk=None):
        self.task_id = task_id
        self.conn = self.get_connection(conn_id)
        self.query_sql = query_sql
        self.split_pk = split_pk
        self.hive_db = hive_db
        self.hive_table = hive_table
        self.hive_table_partition = hive_table_partition
        self.log.info("Using connection to: {}:{}/{}".format(self.conn.host, self.conn.port, self.conn.schema))
         
        self.hive_table_column = hive_table_column 
        if isinstance(hive_table_column, str):
            self.hive_table_column = []
            cl = hive_table_column.split(',') 
            for item in cl:
                hive_table_column_item = {
                    "name": item,
                    "type": "string"
                }
                self.hive_table_column.append(hive_table_column_item)

    
    def Popen(self, cmd, **kwargs):
        """
        Remote Popen

        :param cmd: command to remotely execute
        :param kwargs: extra arguments to Popen (see subprocess.Popen)
        :return: handle to subprocess
        """
        self.sp = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            **kwargs)

        for line in iter(self.sp.stdout):
            self.log.info(line.strip().decode('utf-8'))

        self.sp.wait()

        self.log.info("Command exited with return code %s", self.sp.returncode)

        if self.sp.returncode:
            raise AirflowException("Execute command failed")



    def generate_setting(self):
        """
         datax速度等设置
        """
        self.setting= {
            "speed": {
                 "byte": 104857600
            },
            "errorLimit": {
                "record": 0,
                "percentage": 0.02
            }
        }
        return self.setting
    
    def generate_reader(self):
        """
        datax reader
        """
        conn_type = 'mysql'
        reader_name = 'mysqlreader'
        if(self.conn.conn_type == 'postgres'):
            conn_type = 'postgresql'
            reader_name = 'postgresqlreader'
        
        self.jdbcUrl =  "jdbc:"+conn_type+"://"+self.conn.host.strip()+":"+str(self.conn.port)+"/"+ self.conn.schema.strip()
        self.reader =  {
            "name": reader_name,
            "parameter": {
                "username": self.conn.login.strip(),
                "password": self.conn.password.strip(),
                "connection": [
                    {
                        "querySql": [
                            self.query_sql
                        ],
                        "jdbcUrl": [
                            self.jdbcUrl
                        ]
                    }
                ]
            }
        }
        
        return self.reader

    def generate_writer(self):
        """
        datax hdafs writer
        """
        self.file_type = "text"
        self.hdfs_path = "/datax/"+self.hive_db+"/"+self.hive_table+"/"+self.hive_table_partition
        self.log.info("临时存储目录：{}".format(self.hdfs_path))
        self.writer = {
                    "name": "hdfswriter",
                    "parameter": {
                        "defaultFS": "hdfs://nameservice1",
                        "hadoopConfig": {
                            "dfs.nameservices": "nameservice1",
                            "dfs.ha.automatic-failover.enabled.nameservice1": True,
                            "ha.zookeeper.quorum": "bigdata2-prod-nn01.ryan-miao.com:2181,bigdata2-prod-nn02.ryan-miao.com:2181,bigdata2-prod-nn03.ryan-miao.com:2181",
                            "dfs.ha.namenodes.nameservice1": "namenode117,namenode124",
                            "dfs.namenode.rpc-address.nameservice1.namenode117": "bigdata2-prod-nn01.ryan-miao.com:8020",
                            "dfs.namenode.servicerpc-address.nameservice1.namenode117": "bigdata2-prod-nn01.ryan-miao.com:8022",
                            "dfs.namenode.http-address.nameservice1.namenode117": "bigdata2-prod-nn01.ryan-miao.com:50070",
                            "dfs.namenode.https-address.nameservice1.namenode117": "bigdata2-prod-nn01.ryan-miao.com:50470",
                            "dfs.namenode.rpc-address.nameservice1.namenode124": "bigdata2-prod-nn02.ryan-miao.com:8020",
                            "dfs.namenode.servicerpc-address.nameservice1.namenode124": "bigdata2-prod-nn02.ryan-miao.com:8022",
                            "dfs.namenode.http-address.nameservice1.namenode124": "bigdata2-prod-nn02.ryan-miao.com:50070",
                            "dfs.namenode.https-address.nameservice1.namenode124": "bigdata2-prod-nn02.ryan-miao.com:50470",
                            "dfs.replication": 3,
                            "dfs.client.failover.proxy.provider.nameservice1": "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
                        },
                        "fileType": self.file_type,
                        "path": self.hdfs_path,
                        "fileName": self.task_id,
                        "column": self.hive_table_column,
                        "writeMode": "nonConflict",
                        "fieldDelimiter": "\t"
                    }
            }
        return self.writer

    def generate_config(self):
        content = [{
            "reader": self.generate_reader(),
            "writer": self.generate_writer()
        }]

        job = {
            "setting": self.generate_setting(),
            "content": content
        }

        config = {
            "job": job
        }

        self.target_json = json.dumps(config)

        # write json to file
        self.json_file= '/tmp/datax_json_'+self.task_id+ uuid.uuid1().hex
        # 打开一个文件
        fo = open(self.json_file, "w")
        fo.write(self.target_json)
        fo.close()
        self.log.info("write config json {}".format(self.json_file))
        return self.json_file

    

    def execute(self, context):
        self.generate_config()
        # check hdfs_path
        hdfs_path = self.hdfs_path
        if(not hdfs_path.startswith('/datax/')):
            raise AirflowException("hdfs路径填写错误，不在/datax目录下")

        # 创建目录
        cmd = ['hadoop', 'fs', '-mkdir', '-p', hdfs_path]
        self.Popen(cmd)

        # 删除文件
        if(not hdfs_path.startswith('/datax/')):
            raise AirflowException("hdfs路径填写错误，不在/datax目录下")
        
        files_path = hdfs_path+"/*";
        try:
            cmd = ['hadoop', 'fs', '-rm', files_path]
            self.Popen(cmd)
        except Exception:
            self.log.info('ignore err, just make sure the dir is clean')
            pass
        
        
        # 上传文件
        datax_home = '/data/opt/datax/bin'
        cmd = [ 'python', datax_home + '/datax.py', self.json_file]
        self.Popen(cmd)
        # 删除配置文件
        os.remove(self.json_file)

        # hive加载
        #hive load data from hdfs
        hql = "LOAD DATA INPATH '"+ hdfs_path + "' OVERWRITE  INTO TABLE " \
              + self.hive_db+"."+self.hive_table + " PARTITION (bizdate="+ self.hive_table_partition  +")"
        cmd = ['hive', '-e', "\"" + hql + "\""]
        self.Popen(cmd)

