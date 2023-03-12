from .job import Job, JobNotFound
from .session import LocalSession, SSHSession
from . import utils


class SlurmAcct:
    def __init__(self, session):
        self.session = session

    def filter(self, job=None, user_id=None):
        if type(job) is int:
            job_id = job
        else:
            job_id = job.job_id
        acct = self.session.sacct(job_id=job_id, user_id=user_id)
        return [Job.from_queue(entry["JOB_ID"], "single", entry) for entry in acct]

    def all(self):
        return self.filter()


class SlurmQueue:
    def __init__(self, session):
        self.session = session

    def submit(self, jobspec, wait=False):
        if type(jobspec) is str:
            jobexec = jobspec
            jobargs = {}
        else:
            jobexec = jobspec.exec
            jobargs = jobspec.construct_args()
        return self.session.sbatch(jobexec, wait=wait, **jobargs)

    def _queue_entries_to_jobs(self, in_queue, dedup=True):
        hetjobs = {}
        merged_jobs = []
        for entry in in_queue:
            job_id, hetidx = utils.parse_job_id(entry["JOBID"])
            if hetidx is not None:
                if job_id not in hetjobs:
                    hetjob = Job.from_queue(job_id, "heterogenous", entry)
                    hetjobs[job_id] = hetjob
                    merged_jobs.append(hetjob)

                hetjobs[job_id].children.append(
                    Job.from_queue(job_id, "single", entry, hetidx=hetidx)
                )

                if not dedup:
                    merged_jobs.append(
                        Job.from_queue(job_id, "single", entry, hetidx=hetidx)
                    )

            else:
                merged_jobs.append(Job.from_queue(job_id, "single", entry))
        return merged_jobs

    def filter(self, job=None, user_id=None):
        if job:
            if type(job) is int:
                job_id = job
            else:
                job_id = job.job_id
        else:
            job_id = None

        return self._queue_entries_to_jobs(
            self.session.squeue(job_id=job_id, user_id=user_id)
        )

    def all(self):
        return self.filter()


class Slurm:
    def __init__(self, session):
        self.session = session
        self.queue = SlurmQueue(self.session)
        self.acct = SlurmAcct(self.session)

    @classmethod
    def ssh(cls, host, username=None, password=None):
        sshargs = {}
        if username:
            sshargs["username"] = username
        if password:
            sshargs["password"] = password
        return cls(SSHSession(host, **sshargs))

    @classmethod
    def local(cls):
        return cls(LocalSession())

    def submit(self, jobspec, wait=False, **kwargs):
        job_id = self.queue.submit(jobspec, wait=wait, **kwargs)
        submitted = self.filter(job=job_id)
        if submitted is None:
            raise JobNotFound
        else:
            return submitted

    def queued(self):
        return self.queue.all()

    def filter(self, job=None, user_id=None):
        in_queue = self.queue.filter(job=job, user_id=user_id)
        if in_queue:
            return in_queue
        return self.acct.filter(job=job, user_id=user_id)
