# -*- coding: utf-8 -*-
#

from hooks.notify_hook import NotifyHook
from airflow.operators.bash_operator import BaseOperator
from airflow.utils.decorators import apply_defaults


class NotifyOperator(BaseOperator):
    """
    使用罗盘通知服务发送通知

    :param message: 内容
    :type message: str or dict
    :param receivers: 英文逗号分割的罗盘账号
    :type receivers: str
    :param subject: 邮件主题
    :type subject: str
    """
    template_fields = ('message','receivers', 'subject')

    @apply_defaults
    def __init__(self,
                 subject=None,
                 message=None,
                 receivers=None,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.message = message
        self.receivers = receivers
        self.subject = subject

    def execute(self, context):
        self.log.info('Sending notify message. receivers:{}  message:{}'.format(self.receivers, self.message))
        hook = NotifyHook(
            subject=self.subject,
            message=self.message,
            receivers=self.receivers
        )
        hook.send()
