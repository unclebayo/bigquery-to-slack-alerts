from google.cloud import bigquery
from google.cloud import pubsub_v1
import croniter
import datetime

# Instantiates a Pub/Sub client
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path('bundle-app-production', 'FraudTopic')

def get_alerts_to_execute(request):
    client = bigquery.Client()
    now = datetime.datetime.now()

    # Perform a query.
    QUERY = ('SELECT name, cron, last_execution FROM `bundle-app-production.fraud.master`')
    query_job = client.query(QUERY)  # API request
    rows = query_job.result()  # Waits for query to finish

    count = 0
    for row in rows:
        target_execution = croniter.croniter(row.cron, now).get_prev(datetime.datetime)
        if row.last_execution is None or target_execution > row.last_execution:
            count+=1
            
            ### Send to Pub / Sub
            data = f"{row.name}"
            future = publisher.publish(topic_path, data.encode("utf-8")) # Data must be a bytestring
            print(future.result())

    return f'{count} alerts to execute'