import asyncio
import sys
import os
import lsst.daf.butler as dafButler
from dataclasses import dataclass
from datetime import date, timedelta
import requests

from queries import (
    get_next_visit_events,
    get_timeout_from_loki,
)


def make_summary_message(day_obs, instrument):
    """Make Prompt Processing summary message for a night

    Parameters
    ----------
    day_obs : `str`
        day_obs in the format of YYYY-MM-DD.
    """

    output_lines = []

    day_obs_int = int(day_obs.replace("-", ""))

    butler_alias = "embargo"
    if instrument == "LATISS":
        survey = "BLOCK-306"
    elif instrument == "LSSTComCam":
        survey = "BLOCK-320"
    else:
        survey = "BLOCK-365"
    next_visits, canceled_visits = asyncio.run(
        get_next_visit_events(day_obs, instrument, survey)
    )
    total_visit_count = len(next_visits)
    canceled_list = next_visits.index.intersection(
        canceled_visits.set_index("groupId").index
    ).tolist()
    if canceled_list:
        next_visits = next_visits.drop(canceled_list)
    butler_nocollection = dafButler.Butler(butler_alias)
    raw_exposures = butler_nocollection.query_dimension_records(
        "exposure",
        instrument=instrument,
        where=f"day_obs={day_obs_int} AND exposure.can_see_sky AND exposure.observation_type='science'",
        explain=False,
    )

    # Do not send message if there are no on-sky exposures.
    if len(raw_exposures) == 0:
        sys.exit(0)

    output_lines.append("Number of on-sky exposures: {:d}".format(len(raw_exposures)))

    raw_exposures = butler_nocollection.query_dimension_records(
        "exposure",
        instrument=instrument,
        where=f"day_obs=day_obs_int AND exposure.science_program IN (survey)",
        bind={"day_obs_int": day_obs_int, "survey": survey},
        explain=False,
    )

    raw_counts = count_datasets(
        butler_nocollection,
        "raw",
        f"{instrument}/raw/all",
        instrument=instrument,
        where=f"day_obs=day_obs_int AND exposure.science_program IN (survey) AND detector < 189",
        bind={"day_obs_int": day_obs_int, "survey": survey},
    )
    output_lines.append(
        f"Number for {survey}: {len(next_visits)}/{total_visit_count} nextVisit, "
        f"{len(raw_exposures):d} raws ({raw_counts} images)"
    )

    if len(raw_exposures) == 0:
        return "\n".join(output_lines)

    try:
        collections = butler_nocollection.collections.query(
            f"{instrument}/prompt/output-{day_obs:s}"
        )
        collection = list(collections)[0]
    except dafButler.MissingCollectionError:
        output_lines.append(f"No output collection was found for {day_obs:s}")
        return "\n".join(output_lines)

    isr_counts = len(
        butler_nocollection.query_datasets(
            "isr_log",
            collections=f"{instrument}/prompt/output-{day_obs:s}/Isr/*",
            where=f"exposure.science_program IN (survey)",
            bind={"survey": survey},
            find_first=False,
            explain=False,
        )
    )
    sfm_counts = len(
        butler_nocollection.query_datasets(
            "isr_log",
            collections=f"{instrument}/prompt/output-{day_obs:s}/SingleFrame*",
            where=f"exposure.science_program IN (survey)",
            bind={"survey": survey},
            find_first=False,
            explain=False,
        )
    )
    dia_counts = len(
        butler_nocollection.query_datasets(
            "isr_log",
            collections=f"{instrument}/prompt/output-{day_obs:s}/ApPipe*",
            where=f"exposure.science_program IN (survey)",
            bind={"survey": survey},
            find_first=False,
            explain=False,
        )
    )

    b = dafButler.Butler(
        butler_alias, collections=[collection, f"{instrument}/defaults"]
    )

    log_visit_detector = set(
        [
            (x.dataId["exposure"], x.dataId["detector"])
            for x in b.query_datasets(
                "isr_log",
                where=f"exposure.science_program IN (survey)",
                bind={"survey": survey},
            )
        ]
    )
    output_lines.append(
        "Number of main pipeline runs: {:d} total, {:d} Isr, {:d} SingleFrame, {:d} ApPipe".format(
            len(log_visit_detector), isr_counts, sfm_counts, dia_counts
        )
    )

    isr_outputs = len(
        b.query_datasets(
            "post_isr_image",
            where=f"exposure.science_program IN (survey)",
            bind={"survey": survey},
            explain=False,
        )
    )
    output_lines.append(
        "- Isr: {:d} attempts, {:d} succeeded.".format(
            isr_counts + sfm_counts + dia_counts, isr_outputs
        )
    )

    sfm_outputs = len(
        b.query_datasets(
            "initial_photometry_match_detector",
            where=f"exposure.science_program IN (survey)",
            bind={"survey": survey},
            explain=False,
        )
    )
    output_lines.append(
        "- ProcessCcd: {:d} attempts, {:d} succeeded, {:d} failed.".format(
            sfm_counts + dia_counts, sfm_outputs, sfm_counts + dia_counts - sfm_outputs
        )
    )

    dia_visit_detector = set(
        [
            (x.dataId["visit"], x.dataId["detector"])
            for x in b.query_datasets(
                "apdb_marker",
                where=f"exposure.science_program IN (survey)",
                bind={"survey": survey},
                explain=False,
            )
        ]
    )
    caveat = (
        "(some of which can be no-work-found)"
        if dia_counts - len(dia_visit_detector) > 0
        and len(dia_visit_detector) < sfm_outputs - sfm_counts
        else ""
    )
    output_lines.append(
        "- ApPipe: {:d} attempts, {:d} succeeded, {:d} failed {:s}.".format(
            dia_counts,
            len(dia_visit_detector),
            dia_counts - len(dia_visit_detector),
            caveat,
        )
    )

    output_lines.append(
        f"<https://usdf-rsp.slac.stanford.edu/times-square/github/lsst-dm/vv-team-notebooks/PREOPS-prompt-error-msgs?day_obs={day_obs}&instrument={instrument}&ts_hide_code=1&survey={survey}|Full Error Log>"
    )

    raws = {r.id: r.group for r in raw_exposures}
    log_group_detector = {
        (raws[visit], detector) for visit, detector in log_visit_detector
    }

    output_lines.append(
        f"<https://usdf-rsp.slac.stanford.edu/times-square/github/lsst-sqre/times-square-usdf/prompt-processing/groups?date={day_obs}&instrument={instrument}&survey={survey}&mode=DEBUG&ts_hide_code=1|Timing plots>"
    )

    return "\n".join(output_lines)


def count_datasets(butler, dataset_type, collection, **kwargs):
    try:
        refs = butler.query_datasets(
            dataset_type,
            collections=collection,
            find_first=False,
            explain=False,
            **kwargs,
        )
    except dafButler.MissingCollectionError:
        return 0
    return len(refs)


if __name__ == "__main__":
    instrument = os.getenv("INSTRUMENT")
    if not instrument:
        instrument = "LSSTCam"
    webhook = "SLACK_WEBHOOK_URL_" + instrument.upper()
    url = os.getenv(webhook)

    day_obs = date.today() - timedelta(days=1)
    day_obs_string = day_obs.strftime("%Y-%m-%d")
    summary = make_summary_message(day_obs_string, instrument)
    output_message = (
        f":clamps: *{instrument} {day_obs.strftime('%A %Y-%m-%d')}* :clamps: \n"
        + summary
    )

    if not url:
        print(f"Must set environment variable {webhook} in order to post")
        print("Message: ")
        print(output_message)
        sys.exit(1)

    res = requests.post(
        url, headers={"Content-Type": "application/json"}, json={"text": output_message}
    )

    if res.status_code != 200:
        print("Failed to send message")
        print(res)
