==========================================================
wormulon - Control Your SLURM Jobs Using Python
==========================================================

wormulon is a Python library aimed at programmatically controlling SLURM jobs
remotely.
Wormulon uses `Paramiko <https://www.paramiko.org/>`_ to connect to a remote
machine via SSH and then wrap SLURM's terminal commands (`sbatch`, `squeue`,
`sacct`, etc.).


Getting Started
===============

You can install wormulon using `pip`:

.. code-block:: bash

    $ pip install -u wormulon


Connect to a remote cluster via SSH:

.. code-block:: python

    import wormulon
    slurm = wormulon.Slurm.ssh(host="example.org", username="me")


Submitting and looking up jobs
------------------------------

.. code-block:: python

    # submits a job using the file `jobscript.sh` in your home directory as the
    # slurm jobscript.
    # Returns an object representing the submitted job
    job = slurm.submit_job("~/jobscript.sh")
    print(job)
    # returns all jobs currently in SLURM's queue by the user 'me'
    my_jobs = slurm.queue.find(user_id="me")
    print(my_jobs)


Features
========

Currently supported:
--------------------

* Talking to Slurm on a remote cluster via SSH.
* Submitting simple jobs with jobscripts and additional specifications (see the 
  :code:`jobspec.Jobspec` class).
* Looking up simple and heterogeneous jobs, currently in Slurm's queue.

Planned:
-----------------

* Actual documentation and more examples.
* Talking to a local Slurm on the local machine.
* Submitting more complex job workflows with job dependencies.
* Submitting and looking up array jobs.
* Submitting heterogeneous jobs.