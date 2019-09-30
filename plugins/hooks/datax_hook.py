# -*- coding: utf-8 -*-
#


"""
This module contains a sqoop 1.x hook
"""
import subprocess
import uuid

from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from copy import deepcopy


class DataxHook(BaseHook):
    """
    Datax执行器
    :param target_json: 配置文件json
    :type target_json str  
    """

    def __init__(self, target_json=None):
        # No mutable types in the default parameters
        self.target_json=target_json
        self.log.info("开始执行datax" )


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
            raise AirflowException("Datax command failed")



    def execute(self):
        datax_home = '/data/opt/datax/bin'
        # write json to file
        json_file= '/tmp/datax_'+uuid.uuid1().hex
        # 打开一个文件
        fo = open(json_file, "w")
        fo.write(self.target_json)
        fo.close()
        cmd = [ 'python', datax_home + '/datax.py', json_file]
        self.Popen(cmd)
