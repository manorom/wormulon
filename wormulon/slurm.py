from .job import Job, JobNotFound
from .session import LocalSession, SSHSession
from . import utils


class SlurmAcct:
    """
    A class respreseting the Slurm accounting log, abstracting the sacct
    command.
    """

    def __init__(self, session):
        self.session = session

    def _acct_entries_to_jobs(self, in_acct, merge=True):
        pass

    def jobs_raw(self, job_id=None, user_id=None):
        return self.session.sacct(job_id, user_id)

    def find(self, job=None, user_id=None):
        """
        Find (multiple) jobs in the Slurm accounting log, optionally filtering
        by job id or user id.

        TODO: document time restricitons if job= is not set.

        Args:
            job_id (int, optional): The ID of the job to find. Defaults to None.
            user_id (str, optional): The ID of the user to find jobs for.
                Defaults to None.

        Returns:
            list: A list of Job objects matching the specified criteria.
        """
        if isinstance(job, int):
            job_id = job
        else:
            job_id = job.job_id
        return self._acct_entries_to_jobs(self.jobs_raw(job_id, user_id))

    def all(self):
        return self.find()

    def get(self, job, raise_not_found=False):
        jobs = self.find(job)
        if not jobs and raise_not_found:
            raise JobNotFound()
        if not jobs:
            return None
        return jobs[0]


class SlurmQueue:
    """
    A class representing the Slurm queue.

    The SlurmQueue class provides a high level abstraction for the Slurm queue
    and supports querying currently pending and active jobs using the squeue
    command, as well as submitting new jobs using the sbatch command.

    Attributes:
        session (SlurmSession): The session associated with the SlurmQueue
            instance.

    """

    def __init__(self, session):
        self.session = session

    def submit(self, jobspec, wait=False):
        """
        Submit a new job to the batch system (using sbatch internally).

        Args:
            jobspec (str or JobSpec): The job specification. Can be a string
                representing the job script or an instance of the JobSpec class.
            wait (bool, optional): Whether to wait for the job to complete
                before returning. Defaults to False.


        Returns:
            int: The job ID assigned to the submitted job.

        """
        if isinstance(jobspec, str):
            jobexec = jobspec
            jobargs = {}
        else:
            jobexec = jobspec.exec
            jobargs = jobspec.construct_args()
        return self.session.sbatch(jobexec, wait=wait, **jobargs)

    def _queue_entries_to_jobs(self, in_queue, merge=True):
        hetjobs = {}
        merged_jobs = []
        for entry in in_queue:
            job_id, job_leader_id, hetidx = utils.parse_job_id(entry["JOBID"])
            if hetidx is not None:
                job = Job.from_queue(job_id, "single", entry, hetidx=hetidx)

                if job_leader_id not in hetjobs:
                    hetjobs[job_leader_id] = [job]
                else:
                    hetjobs[job_leader_id].append(job)

                if hetidx == 0 and merge:
                    hetjob = Job.from_queue(job_leader_id, "heterogeneous", entry)
                    hetjob.children = hetjobs[job_leader_id]
                    merged_jobs.append(hetjob)

                if not merge:
                    merged_jobs.append(
                        Job.from_queue(job_id, "single", entry, hetidx=hetidx)
                    )
            else:
                merged_jobs.append(Job.from_queue(job_id, "single", entry))
        return merged_jobs

    def find(self, job=None, user_id=None):
        """
        Find (multiple) jobs in the Slurm queue, optionally filtering by job id
        or user id.

        Jobs in the queue are either active (configuring, running, completing)
        or are scheduled to run in the future.

        Args:
            job_id (int, optional): The ID of the job to find. Defaults to None.
            user_id (str, optional): The ID of the user to find jobs for.
                Defaults to None.

        Returns:
            list: A list of Job objects matching the specified criteria.
        """
        if job:
            if isinstance(job, int):
                job_id = job
            else:
                job_id = job.job_id
        else:
            job_id = None

        return self._queue_entries_to_jobs(
            self.session.squeue(job_id=job_id, user_id=user_id)
        )

    def get(self, job, raise_not_found=False):
        """
        Get information about a specific (single) job.

        Args:
            job (int or Job): The job ID or Job object to retrieve information
                for.
            raise_not_found (bool, optional): Whether to raise an exception if
                the job is not found. Defaults to False.

        Returns:
            Job or None: The Job object representing the job information, or
                None if the job is not found (and raise_not_found is False).

        Raises:
            JobNotFound: If the job is not found and raise_not_found is True.

        """
        jobs = self.find(job)
        if not jobs and raise_not_found:
            raise JobNotFound()
        if not jobs:
            return None
        return jobs[0]

    def all(self):
        """
        Get all jobs currently in the queue.

        Returns:
            list: A list of Job objects representing all jobs the queue.

        """
        return self.find()


class Slurm:
    def __init__(self, session):
        """
        Initialize a Slurm API instance.

        Args:
            session (Session): The session object to establish the connection.
        """
        self.session = session
        self.queue = SlurmQueue(self.session)
        self.acct = SlurmAcct(self.session)

    @classmethod
    def ssh(cls, host, username=None, password=None):
        """ "
        Create a Slurm API instance using SSH session.

        Args:
            host (str): The hostname or IP address of the remote server.
            username (str, optional): The SSH username.
            password (str, optional): The SSH password.

        Returns:
            Slurm: An instance of the Slurm API.
        """
        sshargs = {}
        if username:
            sshargs["username"] = username
        if password:
            sshargs["password"] = password
        return cls(SSHSession(host, **sshargs))

    @classmethod
    def local(cls):
        """
        Create a Slurm API instance for the local machine.

        Returns:
            Slurm: An instance of the Slurm API.
        """
        return cls(LocalSession())

    def submit_job(self, jobspec, wait=False, **kwargs):
        """
        Submit a job to SLURM's job queue.

        Args:
            jobspec (Jobspec|str): The job specification or path to a batch script.
            wait (bool, optional): Whether to wait for job completion. Defaults to False.
            **kwargs: Additional arguments to pass to the job submission.

        Returns:
            Job: The submitted job object.

        Raises:
            JobNotFound: If the job cannot be found in the queue after
            submission.
        """
        job_id = self.queue.submit(jobspec, wait=wait, **kwargs)
        submitted = self.find_job(job=job_id)
        if submitted is None:
            raise JobNotFound
        else:
            return submitted

    def cancel_job(self, job):
        """
        Cancels a job that is currently queued

        Args:
            job (Job|int):Job object or job id of the job to be cancelled
        """
        if isinstance(job, Job):
            job_id = job.job_id
        else:
            job_id = job

        self.session.scancel(job_id)

    def queued_jobs(self):
        """
        Retrieve a list of all jobs in the Slurm queue.

        Returns:
            list: A list of queued jobs.
        """
        return self.queue.all()

    def find_job(self, job=None, user_id=None):
        """
        Filter jobs based on job ID or user ID.

        Args:
            job (str, optional): The job ID to filter. Defaults to None.
            user_id (str, optional): The user ID to filter. Defaults to None.

        Returns:
            Job or None: The filtered job object or None if not found.
        """
        in_queue = self.queue.find(job=job, user_id=user_id)
        if in_queue:
            return in_queue
        return self.acct.find(job=job, user_id=user_id)
