# coding: utf-8

from __future__ import division, print_function, unicode_literals, absolute_import

"""
This module defines tasks to run FEFF.
"""

import os
import shutil
import subprocess

from fireworks import explicit_serialize, FiretaskBase

from atomate.utils.utils import env_chk, get_logger

__author__ = 'Kiran Mathew'
__email__ = "kmathew@lbl.gov"

logger = get_logger(__name__)


@explicit_serialize
class RunFeffDirect(FiretaskBase):
    """
    Run FEFF.

    Required params:
        feff_cmd (str): the name of the full executable for running FEFF (supports env_chk)
    """

    required_params = ["feff_cmd"]

    def run_task(self, fw_spec):
        feff_cmd = env_chk(self["feff_cmd"], fw_spec)
        logger.info("Running FEFF using exe: {}".format(feff_cmd))
        return_code = subprocess.call(feff_cmd, shell=True)
        logger.info("FEFF finished running with returncode: {}".format(return_code))


@explicit_serialize
class RunFeffFake(FiretaskBase):
    """
    Pretend run i.e just copy files from existing run dir.

    Required params:
        ref_dir (str): path to the reference dir
    """

    required_params = ["ref_dir"]

    def run_task(self, fw_spec):
        output_dir = os.path.abspath(self["ref_dir"])
        for file_name in os.listdir(output_dir):
            full_file_name = os.path.join(output_dir, file_name)
            if os.path.isfile(full_file_name):
                shutil.copy(full_file_name, os.getcwd())
        logger.info("Ran fake FEFF.")