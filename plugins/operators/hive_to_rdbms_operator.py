# -*- coding: utf-8 -*-
#
#

"""
hive出库到pg或mysql
"""
import os
import signal

from hooks.hive_to_rdbms_hook import Hive2RDBMSHook
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class Hive2RDBMSOperator(BaseOperator):
    """
    hive出库
    1. hive -e "select a,b,c from tbl where xxx limit 123"
    2. datax text reader and mysql writer
    3. delete text

    https://github.com/alibaba/DataX

    :param hive_query_sql: hive查询语句
    :param rdbms_conn_id   : 出库连接
    :param rdbms_table:  出库table
    :param rdbms_column : 出库列 ['id', 'name']
    :param rdbms_presql : 出库前执行
    """
    template_fields = ('hive_query_sql', 'rdbms_presql')
    ui_color = '#d7f3a7'

    @apply_defaults
    def __init__(self,
                 hive_query_sql,
                 rdbms_conn_id,
                 rdbms_table,
                 rdbms_column,
                 rdbms_presql,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.hive_query_sql = hive_query_sql
        self.rdbms_conn_id = rdbms_conn_id
        self.rdbms_table = rdbms_table
        self.rdbms_column = rdbms_column
        self.rdbms_presql = rdbms_presql
        

    def execute(self, context):
        """
        Execute 
        """
        task_id = context['task_instance'].dag_id + "#" + context['task_instance'].task_id

        self.hook = Hive2RDBMSHook(
                        task_id = task_id,
                        hive_query_sql = self.hive_query_sql,
                        rdbms_conn_id=self.rdbms_conn_id, 
                        rdbms_table=self.rdbms_table, 
                        rdbms_column=self.rdbms_column,
                        rdbms_presql=self.rdbms_presql
                        )
        self.hook.execute(context=context)

        
    def on_kill(self):
        self.log.info('Sending SIGTERM signal to bash process group')
        os.killpg(os.getpgid(self.hook.sp.pid), signal.SIGTERM)
