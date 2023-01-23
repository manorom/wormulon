from dataclasses import dataclass
import datetime
import time
from .session import SlurmError


class JobNotFound(SlurmError):
    pass


def _squeue_entry_is_done(state):
    return state in {
        "BOOT_FAIL",
        "CANCELLED",
        "COMPLETED",
        "DEALINE",
        "FAILED",
        "NODE_FAIL",
        "OUT_OF_MEMORY",
        "PREEMPTED",
        "TIMEOUT",
    }


def _parse_date(datestr):
    if datestr in ("Unknown", "N/A"):
        return None
    else:
        return datetime.datetime.fromisoformat(datestr)


@dataclass
class Job:
    account: str
    job_id: str
    state: str
    end_time: datetime.datetime | None

    def _update(self, session, raise_not_found=True):
        queue_entry = session.squeue(job_id=self.job_id)
        if len(queue_entry) > 0:
            self.state = queue_entry[0]["STATE"]
            self.end_time = _parse_date(queue_entry[0]["END_TIME"])
            return
        acct_entry = session.sacct(job_id=self.job_id)
        if len(acct_entry) > 0:
            self.state = acct_entry[0]["STATE"]
            self.end_time = _parse_date(acct_entry[0]["END_TIME"])
            return
        if raise_not_found:
            raise JobNotFound()

    @classmethod
    def from_queue(cls, squeue_output):
        return cls(
            account=squeue_output["ACCOUNT"],
            job_id=squeue_output["JOBID"],
            state=squeue_output["STATE"],
            end_time=_parse_date(squeue_output["END_TIME"]),
        )

    def poll_to_completion(self, session, interval=10):
        while True:
            squeue_entries = session.squeue(job_id=self.job_id)
            if len(squeue_entries) == 0:
                self._update(session)
                return True
            time.sleep(interval)

    def cancel(self, session):
        session.scancel(job_id=self.job_id)

    @property
    def running(self):
        return self.state == "RUNNING"

    @property
    def pending(self):
        return self.state == "PENDING"

    @property
    def done(self):
        return _squeue_entry_is_done(self.state)
