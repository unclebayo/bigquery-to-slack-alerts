from google.cloud import bigquery
from google.cloud import pubsub_v1

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

import base64
import croniter
import datetime
import os
import json
import time

slack_token = os.environ["SLACK_BOT_TOKEN"]
slack_client = WebClient(token=slack_token)


def send_to_slack(msg, channel):
    try:
        response = slack_client.chat_postMessage(channel=channel, text=msg)
    except SlackApiError as e:
        # You will get a SlackApiError if "ok" is False
        assert e.response["ok"] is False
        assert e.response["error"]  # str like 'invalid_auth', 'channel_not_found'
        print(f"Got an error: {e.response['error']}")

        
def execute_alert(name):
    client = bigquery.Client()
    now = datetime.datetime.now()

    # Perform a query.
    MASTER_QUERY = (f'SELECT sql, intro_msg, row_msg, channel FROM `bundle-app-production.fraud.master` WHERE name = "{name}" LIMIT 1')
    master_query_job = client.query(MASTER_QUERY)  # API request
    master_rows = master_query_job.result()  # Waits for query to finish
    alert_infos = next(master_rows)
    
    ALERT_QUERY = alert_infos.sql
    alert_query_job = client.query(ALERT_QUERY)  # API request
    alert_rows = alert_query_job.result()  # Waits for query to finish

    row_count = 0
    for row in alert_rows:
        keys = list(row.keys())
        row_msg = alert_infos.row_msg
        intro_msg = alert_infos.intro_msg
        channel = alert_infos.channel

        for key in keys:
            row_msg = row_msg.replace('{' + str(key) + '}', str(row[key]))

        ## Send alert to Slack with intromessage if rowcount = 1
        if (row_count == 0 and alert_infos.intro_msg is not None):
            send_to_slack(alert_infos.intro_msg, channel)

        send_to_slack(row_msg, channel)
        row_count += 1

    ## Update last execution date in master table
    MASTER_UPDATE = (f'UPDATE `bundle-app-production.fraud.master` SET last_execution = "{now}" WHERE name = "{name}"')
    master_update_job = client.query(MASTER_UPDATE)  # API request
    master_update_job.result()  # Waits for query to finish



def create_alert_button(name):
    message_blocks = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"Alert ID: {name} This alert needs attention. Click the button below to mark it as closed."
            }
        },
        {
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {
                        "type": "plain_text",
                        "text": "Close Alert"
                    },
                    "value": "name",
                    "action_id": "close_alert"
                }
            ]
        }
    ]
    
    message = {'blocks': message_blocks}

    try:
        response = slack_client.chat_postMessage(
            channel="#fraud-alerting-system",
            blocks=json.dumps(message['blocks'])
        )
        print(f"Alert sent with timestamp: {response['ts']}")
    except SlackApiError as e:
        print("Error sending alert: {}".format(e))

def handle_alert_button(payload):
    alert_id = json.loads(payload)['actions'][0]['value']
    action_id = json.loads(payload)['actions'][0]['action_id']
    if action_id == "close_alert":
        # Update the status of the alert in BigQuery
        client = bigquery.Client()
        update_query = f'UPDATE `bundle-app-production.fraud.master` SET status = "closed" WHERE name = "{name}"'
        update_job = client.query(update_query)  # API request
        update_job.result()  # Waits for query to finish
        print(f"Alert {name} has been marked as closed.")

def trigger_pubsub(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    execute_alert(pubsub_message)
    create_alert_button(pubsub_message)
 
    # Register an event listener for the button click event
    slack_client.api_call(
        "event_subscriptions.add",
        **{
            "url": "https://us-central1-bundle-app-production.cloudfunctions.net/FraudAlertsProducer",
            "event_types": ["block_actions"],
        }
    )