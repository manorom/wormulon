import paramiko
from typing import Optional
import abc

_SQUEUE_FORMAT = "%a|%A|%T|%B|%e"
_SQUEUE_FORMAT_KEYS = ("ACCOUNT", "JOBID", "STATE", "EXEC_HOST", "END_TIME")
_SACCT_FORMAT_KEYS = ("ACCOUNT", "JOBID", "STATE", "END_TIME")


class SlurmError(Exception):
    pass


class Session(abc.ABC):
    @abc.abstractmethod
    def exec_cmd(self, cmd: str):
        pass

    def sbatch(self, jobexec: str, **kwargs) -> int:
        args = " ".join(
            map(lambda kv: f"--{kv[0]}={kv[1]}" if kv[1] else f"--{kv[0]}", kwargs.items())
        )
        out, err, exit_code = self.exec_cmd(f"sbatch {jobexec} " + args)
        if exit_code != 0:
            raise SlurmError(err)
        return int(out.strip().split(" ")[-1])

    def squeue(self, user_id: Optional[str] = None, job_id: Optional[int] = None):
        uid_flag = ""
        job_id_flag = ""
        if user_id is not None:
            uid_flag = f"-u {user_id} "
        if job_id is not None:
            job_id_flag = f"-j {job_id} "

        args = f'squeue {uid_flag}{job_id_flag} -o "{_SQUEUE_FORMAT}" -h'
        out, err, exit_code = self.exec_cmd(args)
        if exit_code != 0:
            raise SlurmError(err)
        # parsing squeue output
        out = out.strip()
        if out == "":
            return []
        return [
            dict(zip(_SQUEUE_FORMAT_KEYS, line.split("|"))) for line in out.split("\n")
        ]

    def scancel(self, job_id: int):
        _, err, exit_code = self.exec_cmd(f"scancel {job_id}")
        if exit_code != 0:
            raise SlurmError(err)

    def sacct(self, job_id=None, allocations=True):
        # use -X flag (maybe)
        args = ["sacct", "-n", "-P", "--format", "User,JobID,State,End"]
        if allocations:
            args.append("-X")
        if job_id:
            args.append(f"-j {job_id}")

        out, err, return_code = self.exec_cmd(" ".join(args))
        if return_code != 0:
            raise SlurmError(err)
        out = out.strip()
        if out == "":
            return []
        entries = []
        for line in out.split("\n"):
            entry = dict(zip(_SACCT_FORMAT_KEYS, line.split("|")))
            entries.append(entry)
        return entries


class SSHSession(Session):
    def __init__(self, host, **kwargs):
        self.client = paramiko.SSHClient()
        self.client.load_system_host_keys()
        self.client.connect(host, **kwargs)

    def exec_cmd(self, cmd, timeout=15):
        _, stdout, stderr = self.client.exec_command(cmd, timeout=timeout)
        stdout_output = stdout.read().decode("utf8")
        stderr_output = stderr.read().decode("utf8")
        exit_code = stdout.channel.recv_exit_status()
        return stdout_output, stderr_output, exit_code


class LocalSession(Session):
    def __init__(self):
        raise NotImplementedError

    def exec_cmd(self, cmd):
        raise NotImplementedError
