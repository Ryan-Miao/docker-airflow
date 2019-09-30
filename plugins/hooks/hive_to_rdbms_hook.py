# -*- coding: utf-8 -*-
#


"""
datax 从hive出库
"""
import subprocess
import uuid
import json
import os

from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook


class Hive2RDBMSHook(BaseHook):
    """
    hive出库text
    datax read text
    datax write rdbms
    """

    def __init__(self,
                 task_id,
                 hive_query_sql,
                 rdbms_conn_id,
                 rdbms_table,
                 rdbms_column,
                 rdbms_presql):
        self.task_id = task_id
        self.hive_query_sql = hive_query_sql
        self.rdbms_conn_id = rdbms_conn_id
        self.conn = self.get_connection(rdbms_conn_id)
        self.rdbms_table = rdbms_table
        self.rdbms_presql = rdbms_presql
        self.log.info("Using connection to: {}:{}/{}".format(self.conn.host, self.conn.port, self.conn.schema))
 
        self.hive_table_column = []
        self.rdbms_column = rdbms_column.split(',')
        for i in range(len(self.rdbms_column)):
            hive_table_column_item = {
                "index": i,
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

    
    def write_file(self, src, data):
        self.log.info('will write to {}'.format(src))
        fo = open(src, "w")
        fo.write(data)
        fo.close()
        self.log.info('write done!!!')



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
        datax text reader
        """
        self.reader =  {
                        "name": "txtfilereader",
                        "parameter": {
                            "path": [self.data_file],
                            "encoding": "UTF-8",
                            "column": self.hive_table_column,
                            "fieldDelimiter": "\t"
                        }
                    }
        
        return self.reader

    def generate_writer(self):
        """
        datax rdbms writer
        """
        conn_type = 'mysql'
        writer_name = 'mysqlwriter'
        if(self.conn.conn_type == 'postgres'):
            conn_type = 'postgresql'
            writer_name = 'postgresqlwriter'
        
        self.jdbcUrl =  "jdbc:"+conn_type+"://"+self.conn.host.strip()+":"+str(self.conn.port)+"/"+ self.conn.schema.strip()
        
        self.writer = {
                        "name": writer_name,
                        "parameter": {
                            "username": self.conn.login.strip(),
                            "password": self.conn.password.strip(),
                            "column": self.rdbms_column,
                            "preSql": [
                                self.rdbms_presql
                            ],
                            "connection": [
                                {
                                    "jdbcUrl": self.jdbcUrl,
                                    "table": [
                                        self.rdbms_table
                                    ]
                                }
                            ]
                        }
                    }
        return self.writer

    def generate_config(self):
        """
        构造datax配置文件
        """
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
        self.write_file(src=self.json_file, data=self.target_json)
        self.log.info("write config json {}".format(self.json_file))
        return self.json_file


    def hive_export(self):
        """
        hive 出库
        return self.data_file  text data
        """
        hql = "set hive.cli.print.header=false;" + self.hive_query_sql
        hql_file='/tmp/datax_hql_'+self.task_id+uuid.uuid1().hex
        self.write_file(src=hql_file, data=hql)

        self.data_file= '/tmp/datax_data_'+self.task_id+uuid.uuid1().hex
        bash_shell = "hive -S -f {} | grep -v \"WARN:\"  > {}".format(hql_file, self.data_file)

        bash_shell_file='/tmp/datax_bash_'+self.task_id+uuid.uuid1().hex
        self.write_file(src=bash_shell_file, data=bash_shell)
        
        cmd = ['/bin/bash', bash_shell_file]
        self.Popen(cmd)

        #remove filles
        os.remove(hql_file)
        os.remove(bash_shell_file)

        return self.data_file
    
    def execute(self, context):
        # hive export
        self.hive_export()
        # datax write
        json_file = self.generate_config()
        datax_home = '/data/opt/datax/bin'
        cmd = [ 'python', datax_home + '/datax.py', json_file]
        self.Popen(cmd)
        # 删除配置文件
        os.remove(json_file)

        #delete data file
        os.remove(self.data_file)