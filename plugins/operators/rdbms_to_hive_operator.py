# -*- coding: utf-8 -*-
#
#

"""
postgres或者mysql 入库到hdfs
"""
import os
import signal

from hooks.rdbms_to_hive_hook import RDBMS2HiveHook
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class RDBMS2HiveOperator(BaseOperator):
    """
    传输pg到hive
    https://github.com/alibaba/DataX

    :param conn_id: pg连接id
    :param query_sql : pg查询语句
    :param split_pk  : pg分割主键， NONE表示不分割，指定后可以多线程分割，加快传输
    :param hive_db   : hive的db
    :param hive_table: hive的table
    :param hive_table_column  column数组， column={name:a, type: int} 或者逗号分割的字符串， column=a,b,c
    :param hive_table_partition 分区bizdate值
    """
    template_fields = ('query_sql',  'hive_db', 'hive_table','hive_table_partition')
    ui_color = '#edd5f1'

    @apply_defaults
    def __init__(self,
                 conn_id,
                 query_sql,
                 hive_db,
                 hive_table,
                 hive_table_column,
                 hive_table_partition,
                 split_pk=None,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.query_sql = query_sql
        self.split_pk = split_pk
        self.hive_db = hive_db
        self.hive_table = hive_table
        self.hive_table_column = hive_table_column
        self.hive_table_partition = hive_table_partition
        

    def execute(self, context):
        """
        Execute 
        """
        task_id = context['task_instance'].dag_id + "#" + context['task_instance'].task_id

        self.hook = RDBMS2HiveHook(
                        task_id = task_id,
                        conn_id = self.conn_id,
                        query_sql = self.query_sql, 
                        split_pk=self.split_pk, 
                        hive_db=self.hive_db, 
                        hive_table=self.hive_table,
                        hive_table_column=self.hive_table_column,
                        hive_table_partition=self.hive_table_partition
                        )
        self.hook.execute(context=context)

        
    def on_kill(self):
        self.log.info('Sending SIGTERM signal to bash process group')
        os.killpg(os.getpgid(self.hook.sp.pid), signal.SIGTERM)
