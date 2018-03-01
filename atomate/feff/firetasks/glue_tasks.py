# coding: utf-8

from __future__ import absolute_import, division, print_function, unicode_literals

import logging
from fireworks import explicit_serialize
from fireworks import FiretaskBase, FWAction
from atomate.utils.utils import get_logger
from atomate.common.firetasks.glue_tasks import get_calc_loc, CopyFiles
from fireworks.user_objects.firetasks.filepad_tasks import get_fpad
import six

__author__ = 'Kiran Mathew, Chen Zheng'
__email__ = 'kmathew@lbl.gov, chz022@ucsd.edu'

logging.basicConfig(filename="feff_run.log", level=logging.INFO)
logger = get_logger(__name__)

@explicit_serialize
class CopyFeffOutputs(CopyFiles):
    """
    Copy files from a previous run directory to the current directory.
    Note: must specify either "calc_loc" or "calc_dir" to indicate the directory
        containing the files to copy.

    Optional params:
        calc_loc (str OR bool): if True will set most recent calc_loc. If str
            search for the most recent calc_loc with the matching name.
        calc_dir (str): path to dir that contains VASP output files.
        filesystem (str): remote filesystem. e.g. username@host
        exclude_files (list): list fo filenames to be excluded when copying.
    """

    optional_params = ["calc_loc", "calc_dir", "filesystem", "exclude_files"]

    def run_task(self, fw_spec):

        calc_loc = get_calc_loc(self["calc_loc"], fw_spec["calc_locs"]) if self.get("calc_loc") else {}
        exclude_files = self.get("exclude_files", [])

        self.setup_copy(self.get("calc_dir", None), filesystem=self.get("filesystem", None),
                        exclude_files=exclude_files, from_path_dict=calc_loc)
        self.copy_files()


@explicit_serialize
class DbQueryModuleOutputs(FiretaskBase):
    """
    Query module output files from gridfs and filepad. Write the module output files
    that could be used for further calculation.
    Required_params:
        query_outputs (list): List of outputs files query from gridfs using filepad
        output_identifier (str/list): Identifier label(s) to tag the query module output
                                    files.

    Optional_params:
        filepad_file (str): path to the filepad connection settings file.
        metaquery (dict): Other pymongo query conditions in dict format
    """

    def run_task(self, fw_spec):
        query_outputs = self["query_outputs"]
        output_labels = self["output_identifier"]
        fpad = get_fpad(self.get("filepad_file", None))
        metaquery = self.get("metaquery", None)

        query_identifiers = []
        fw_mod_spec = {'_push_all':dict()}

        for index, output in enumerate(query_outputs):
            query_file = dict()
            query_file["filename"] = output
            query_file["conditions"] = dict()
            if isinstance(output_labels, six.string_types):
                query_file["conditions"]["identifier"] = "-".join((output_labels, output))
            elif isinstance(output_labels, (list,)):
                query_file["conditions"]["identifier"] = output_labels[index]

            if metaquery:
                query_file["conditions"] = {**query_file["conditions"], **metaquery}

            query_identifiers.append(query_file)

        for sub_query in query_identifiers:
            query_results = fpad.get_file_by_query(sub_query["conditions"])
            if len(query_results)>=0:
                with open(sub_query["filename"],"wb") as f:
                    f.write(query_results[0][0])
                fw_mod_spec['_push_all'][sub_query["filename"]] = True

        logger.info("Output files queried from database: {}".format(fw_mod_spec['_push_all']))

        if fw_mod_spec['_push_all']:
            return FWAction(mod_spec=[fw_mod_spec])



