from dataclasses import dataclass
import datetime
import time
from .session import SlurmError
from . import utils


class JobNotFound(SlurmError):
    pass


def _squeue_entry_is_done(state):
    return state in {
        "BOOT_FAIL",
        "CANCELLED",
        "COMPLETED",
        "DEADLINE",
        "FAILED",
        "NODE_FAIL",
        "OUT_OF_MEMORY",
        "PREEMPTED",
        "TIMEOUT",
    }


@dataclass
class Job:
    job_id: str
    kind: str
    heterogenous_index: int | None
    account: str
    partition: str
    state: str
    end_time: datetime.datetime | None
    children: list

    def _update(self, session, raise_not_found=True):
        queue_entry = session.squeue(job_id=self.job_id)
        if len(queue_entry) > 0:
            self.state = queue_entry[0]["STATE"]
            self.end_time = utils.parse_date(queue_entry[0]["END_TIME"])
            return
        acct_entry = session.sacct(job_id=self.job_id)
        if len(acct_entry) > 0:
            self.state = acct_entry[0]["STATE"]
            self.end_time = utils.parse_date(acct_entry[0]["END_TIME"])
            return
        if raise_not_found:
            raise JobNotFound()

    @classmethod
    def from_queue(cls, job_id, kind, squeue_output, hetidx=None):
        return cls(
            job_id=job_id,
            kind=kind,
            heterogenous_index=hetidx,
            account=squeue_output["ACCOUNT"],
            partition=squeue_output["PARTITION"],
            state=squeue_output["STATE"],
            end_time=utils.parse_date(squeue_output["END_TIME"]),
            children=[],
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
    def cancelled(self):
        return self.state == "CANCELLED"

    @property
    def failed(self):
        return self.state == "FAILED"

    @property
    def completed(self):
        return self.state == "COMPLETED"

    @property
    def terminated(self):
        return _squeue_entry_is_done(self.state)

    @property
    def heterogenous(self):
        return self.kind == "heterogenous"

    @property
    def single(self):
        return self.kind == "single"
