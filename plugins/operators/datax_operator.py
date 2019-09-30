# -*- coding: utf-8 -*-
#
#

"""
This module contains a sqoop 1 operator
"""
import os
import signal

from hooks.datax_hook import DataxHook
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataxOperator(BaseOperator):
    """
    执行datax
    https://github.com/alibaba/DataX

    :param target_json: 配置文件json 
    """
    template_fields = ('target_json',)
    ui_color = '#6fac07'

    @apply_defaults
    def __init__(self,
                 target_json,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.target_json = target_json
        

    def execute(self, context):
        """
        Execute sqoop job
        """
        self.hook = DataxHook(self.target_json)
        self.hook.execute()

        
    def on_kill(self):
        self.log.info('Sending SIGTERM signal to bash process group')
        os.killpg(os.getpgid(self.hook.sp.pid), signal.SIGTERM)
