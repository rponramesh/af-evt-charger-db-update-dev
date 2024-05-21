import datetime
import logging
from . import processAPIData
import azure.functions as func


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('going to update the DB!')
    #processAPIData.UpdateDBdriver()


    processAPIData.UpdateEpicData()

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
