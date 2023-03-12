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

    def all(self, user_id=None):
        return [Job.from_queue(e) for e in self.session.sacct(user_id)]


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
