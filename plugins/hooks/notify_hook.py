# -*- coding: utf-8 -*-
#

import json

import requests

from airflow import AirflowException
from airflow.hooks.http_hook import HttpHook


class NotifyHook(HttpHook):
    """
    使用罗盘通知服务发送通知

    :param send_type: 通知类型选填 MAIL,DINGDING,SMS，选填多个时中间用英文逗号隔开
    :type send_type: str
    :param message: 内容
    :type message: str or dict
    :param receivers: 英文逗号分割的罗盘账号
    :type receivers: str
    :param subject: 邮件主题
    :type subject: str
    """

    def __init__(self,
                 notify_conn_id='notify_default',
                 send_type='MAIL',
                 subject=None,
                 message=None,
                 receivers=None,
                 *args,
                 **kwargs
                 ):
        super().__init__(http_conn_id=notify_conn_id, *args, **kwargs)
        self.send_type = send_type
        self.message = message
        self.subject = subject
        self.receivers = receivers


    def _build_message(self):
        """
        构建data
        """
        data = {
                "content": self.message,
                "contentType": "HTML",
                "receivers": self.receivers,
                "sendType": self.send_type,
                "sender": '【Airflow】',
                "subject": '【Airflow】' + self.subject
            }
        return json.dumps(data)

    def get_conn(self, headers=None):
        """
        Overwrite HttpHook get_conn because just need base_url and headers and
        not don't need generic params

        :param headers: additional headers to be passed through as a dictionary
        :type headers: dict
        """
        self.base_url = 'http://notify.ryan-miao.com'
        session = requests.Session()
        if headers:
            session.headers.update(headers)
        return session

    def send(self):
        """
        Send Notify message
        """
        data = self._build_message()
        self.log.info('Sending message: %s',  data)
        resp = self.run(endpoint='/api/v2/notify/send',
                        data=data,
                        headers={'Content-Type': 'application/json',
                                 'app-id': 'compass',
                                 'app-key': '152ccb38cce8a8770d34ff22a09c804c73bbae0f'})

        # Dingding success send message will with errcode equal to 0
        if int(resp.json().get('retCode')) != 0:
            raise AirflowException('Send notify message failed, receive error '
                                   'message %s', resp.text)
        self.log.info('Success Send notify message')
