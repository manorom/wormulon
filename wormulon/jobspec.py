from .job import Job, JobNotFound

from copy import deepcopy


class Jobspec:
    def __init__(self, jobexec):
        self.exec = jobexec
        self._exec_args = {}
        self._dependencies = {}

    def construct_args(self):
        args = deepcopy(self._exec_args)
        deps = []
        for kind, ids in self._dependencies:
            deps.append(f"{kind}:" + ":".join(ids))
        if deps:
            args["dependencies"] = ",".join(deps)
        return args

    def after_ok(self, job):
        if "afterok" in self._dependencies:
            self._dependencies["afterok"].append(job.id)
        else:
            self._dependencies["afterok"] = [job.id]
        return self

    def exclusive(self):
        self._exec_args["exlusive"] = ""
        return self

    def nodes(self, n):
        self._exec_args["nodes"] = n
        return self

    def ntasks(self, n):
        self._exec_args["ntasks"] = n
        return self

    def ntasks_per_node(self, n):
        self._exec_args["ntasks-per-node"] = n
        return self

    def cpus_per_task(self, n):
        self._exec_args["cpus-per-task"] = n
        return self

    def mem_per_cpu(self, n):
        self._exec_args["mem-per-cpu"] = n
        return self

    def job_name(self, name):
        self._exec_args["job-name"] = name
        return self

    def output(self, output):
        self._exec_args["output"] = output
        return self
