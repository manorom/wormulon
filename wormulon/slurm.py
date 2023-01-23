from .job import Job, JobNotFound
from .session import LocalSession, SSHSession


class SlurmAcct:
    def __init__(self, session):
        self.session = session

    def lookup(self, job):
        if type(job) is int:
            job_id = job
        else:
            job_id = job.job_id
        acct = self.session.sacct(job_id=job_id)
        if len(acct) > 0:
            return Job.from_queue(acct[0])
        else:
            return None

    def all(self):
        return [Job.from_queue(e) for e in self.session.sacct()]


class SlurmQueue:
    def __init__(self, session):
        self.session = session

    def submit(self, jobspec, wait=False, **kwargs):
        if type(jobspec) is str:
            jobexec = jobspec
            jobargs = {}
        else:
            jobexec = jobspec.exec
            jobargs = jobspec.construct_args()
        return self.session.sbatch(jobexec, **jobargs)

    def lookup(self, job):
        if type(job) is int:
            job_id = job
        else:
            job_id = job.job_id

        in_queue = self.session.squeue(job_id=job_id)
        if len(in_queue) > 0:
            return Job.from_queue(in_queue[0])
        else:
            return None

    def all(self):
        return [Job.from_queue(e) for e in self.session.squeue()]


# TODO: integrate user_id (required for squeue and sacct)
class Slurm:
    def __init__(
            self, session=None, host=None, username=None, password=None, local=False
    ):
        if session:
            self.session = session
        elif host:
            sshargs = {}
            if username:
                sshargs["username"] = username
            if password:
                sshargs["password"] = password
            self.session = SSHSession(host, **sshargs)
        elif local:
            self.session = LocalSession()
        else:
            raise ValueError(
                "Slurm.__init__ needs to be called with argument session or ssh_host set, or local set to True"
            )
        self.queue = SlurmQueue(self.session)
        self.acct = SlurmAcct(self.session)

    def submit(self, jobspec, wait=False, **kwargs):
        job_id = self.queue.submit(jobspec, wait=wait, **kwargs)
        submitted = self.lookup(job_id)
        if submitted is None:
            raise JobNotFound
        else:
            return submitted

    def queued(self):
        return self.queue.all()

    def lookup(self, job):
        in_queue = self.queue.lookup(job)
        if in_queue:
            return in_queue
        return self.acct.lookup(job)
