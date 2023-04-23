import requests
import pytz
from datetime import datetime
from send_slack_msg import send_slack_notification

TRIMET_DATA_URL = "http://www.psudataeng.com:8000/getBreadCrumbData"
DATASTORE_PATH = "/home/dtm-project/data-archive/"

def retrieve_and_save():
    def get_date_str():
        return datetime.now(tz=pytz.utc).astimezone(pytz.timezone("US/Pacific")).strftime("%Y-%m-%d")
    print("Fetching data...")
    try:
        data = requests.get(TRIMET_DATA_URL).text
        filename = f"{DATASTORE_PATH}{get_date_str()}.json"

        with open(filename, 'w') as writer:
            writer.write(data)
        return f"Today's Trimet data was retrieved and stored into {filename}"
    except:
        return f"There was an issue retrieving or storing today's Trimet data."


if(__name__ == "__main__"):
    result = retrieve_and_save()
    send_slack_notification(result)
