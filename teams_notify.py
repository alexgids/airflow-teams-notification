#!/usr/bin/env python
import json
import requests
import logging
logging.getLogger(__name__).setLevel(logging.INFO)
from airflow.hooks.base_hook import BaseHook




class TeamsNotification:

    def __init__(self,conn_id):
        if conn_id:
            self.webhook_url = BaseHook.get_connection(conn_id).host


    def error_alert(self,context):

        message = {
            "@type": "MessageCard",
            "@context": "http://schema.org/extensions",
            "themeColor": "F7362D",
            "summary": "Airflow Alert",
            "sections": [
              {
                  "activityTitle": ":stop_sign Task Failed",
                  "activityImage": "https://freeiconshop.com/wp-content/uploads/edd/error-flat.png",
                  "activitySubtitle": "Airflow",
                  "facts": [
                      {
                          "name": "DAG",
                          "value": context.get('task_instance').dag_id
                      },
                      {
                          "name": "Task instance",
                          "value": context.get('task_instance').task_id
                      },
                     {
                          "name": "Execution date",
                          "value": context.get('execution_date')
                      },
                     {
                          "name": "Logs",
                          "value": context.get('log_url')
                    }
                  ],
                  "markdown": True
              }
            ],
            "potentialAction": [
                {
                    "@type": "OpenUri",
                    "name": "View Log",
                    "targets": [{"os": "default", "uri": context.get('log_url')}]
                }
            ]
            }
        requests.post(url=self.webhook_url,data=json.dumps(message),headers={"Content-Type": "application/json"})

    def success_alert(self,context):
        message = {
            "@type": "MessageCard",
            "@context": "http://schema.org/extensions",
            "themeColor": "F7362D",
            "summary": "Airflow Alert",
            "sections": [
                {
                    "activityTitle": ":check_mark_button Task Successful",
                    "activityImage": "https://freeiconshop.com/wp-content/uploads/edd/checkmark-flat.png",
                    "activitySubtitle": "Airflow",
                    "facts": [
                        {
                            "name": "DAG",
                            "value": context.get('task_instance').dag_id
                        },
                        {
                            "name": "Task instance",
                            "value": context.get('task_instance').task_id
                        },
                        {
                            "name": "Execution date",
                            "value": context.get('execution_date')
                        },
                        {
                            "name": "Logs",
                            "value": context.get('log_url')
                        }
                    ],
                    "markdown": True
                }
            ],
            "potentialAction": [
                {
                    "@type": "OpenUri",
                    "name": "View Log",
                    "targets": [{"os": "default", "uri": context.get('log_url')}]
                }
            ]
        }
        requests.post(url=self.webhook_url,data=json.dumps(message),headers={"Content-Type": "application/json"})


if __name__ == '__main__':
    pass