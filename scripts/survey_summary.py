import asyncio
import requests
import sys
import os
from lsst.daf.butler import Butler
from datetime import date, timedelta

from queries import (
    get_next_visit_events,
)


if __name__ == "__main__":
    url = os.getenv("SLACK_WEBHOOK_URL_COMCAM")
    instrument = "LSSTComCam"

    day_obs = date.today() - timedelta(days=1)
    day_obs_string = day_obs.strftime("%Y-%m-%d")
    day_obs_int = int(day_obs_string.replace("-", ""))

    df = asyncio.run(get_next_visit_events(day_obs_string, 2))
    df = df[df["survey"] != ""]

    output_lines = []

    butler_alias = "embargo"
    butler_nocollection = Butler(butler_alias)
    raw_exposures = butler_nocollection.query_dimension_records(
        "exposure",
        instrument=instrument,
        where=f"day_obs={day_obs_int} AND exposure.can_see_sky AND exposure.observation_type='science'",
        explain=False,
    )
    output_lines.append(
        "Number of on-sky science exposures: {:d}".format(len(raw_exposures))
    )

    for block in df["survey"].unique():
        count_events = len(df[df["survey"] == block])

        raw_exposures = butler_nocollection.query_dimension_records(
            "exposure",
            instrument=instrument,
            where=f"day_obs=day_obs_int AND exposure.science_program IN (survey)",
            bind={"day_obs_int": day_obs_int, "survey": block},
            explain=False,
        )
        output_lines.append(
            f"{block}: {count_events} nextVisit events. {len(raw_exposures)} raw exposures."
        )

    output_message = (
        f":clamps: *{instrument} {day_obs.strftime('%A %Y-%m-%d')}* :clamps: \n"
        + "\n".join(output_lines)
    )

    if not url:
        print("Must set environment variable SLACK_WEBHOOK_URL in order to post")
        print("Message: ")
        print(output_message)
        sys.exit(1)

    res = requests.post(
        url, headers={"Content-Type": "application/json"}, json={"text": output_message}
    )

    if res.status_code != 200:
        print("Failed to send message")
        print(res)
