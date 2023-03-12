import datetime

def parse_job_id(jobid_str):
    jobidsplit = jobid_str.split("+", 1)
    job_id = jobidsplit[0]
    if len(jobidsplit) > 1:
        hetidx = int(jobidsplit[1])
    else:
        hetidx = None
    return job_id, hetidx


def parse_date(datestr):
    if datestr in ("Unknown", "N/A"):
        return None
    else:
        return datetime.datetime.fromisoformat(datestr)
