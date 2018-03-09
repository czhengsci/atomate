# coding: utf-8

from __future__ import absolute_import, division, print_function, unicode_literals

import logging
from matgendb.util import get_database
from fireworks import explicit_serialize
from fireworks import FiretaskBase, FWAction
from atomate.utils.utils import get_logger
from atomate.common.firetasks.glue_tasks import get_calc_loc, CopyFiles
import six
import os
import subprocess
import re

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
        index_collection (str): Name of the collection in Mongodb stores information, i.e calculation parameters,
                                file pathways, etc., of calculated intermediate file..
        indexdb_settings (str): File path to json file used for index database connection setup


    Optional_params:
        metaquery (dict): Other pymongo query conditions in dict format
        calc_inputparams (dict): Input parameters of calculation used for query results filtering
        calc_inputparams_nest (str/list): Nested fields of calculation input parameters.
                                e.g. if one input parameters is nested as <field1>.<field2>.<param1>:<value1>,
                                the calc_inputparams_nest should be [field1, field2]. This is a temporary
                                solution as dotted field is not valid for mongodb's storage.
    """

    def run_task(self, fw_spec):
        query_outputs = self["query_outputs"]
        output_labels = self["output_identifier"]
        index_collection = self["index_collection"]
        indexdb_settings = self["indexdb_settings"]
        metaquery = self.get("metaquery", dict())

        # Process the input parameters and make them into pymongo nested fields for query purpose
        calc_inputparams = self.get("calc_inputparams", None)
        calc_inputparams_nest = self.get("calc_inputparams_nest", None)

        if calc_inputparams and calc_inputparams_nest:
            for k, v in calc_inputparams.items():
                if isinstance(calc_inputparams_nest, six.string_types):
                    query_key = ".".join((calc_inputparams_nest, k))
                elif isinstance(calc_inputparams_nest, (list,)):
                    query_key = '.'.join((".".join(calc_inputparams_nest), k))
                metaquery[query_key] = v
        elif calc_inputparams and not calc_inputparams_nest:
            metaquery = {**metaquery, **calc_inputparams}

        logger.info("metaquery information is {}".format(metaquery))

        fw_mod_spec = {'_push_all': dict()}
        index_db_connection = get_database(indexdb_settings)
        identifier_pattern = re.compile("{}.*".format(output_labels))
        db_query_cond = {**{"identifier": identifier_pattern}, **metaquery}
        cal_cursor = index_db_connection[index_collection].find(db_query_cond)

        if cal_cursor.count() > 0:
            intermediate_files_field = index_db_connection[index_collection].find_one(db_query_cond,
                                                                                      {"intermediate_files": 1})

            for output_dict in intermediate_files_field["intermediate_files"]:
                if output_dict["original_file_name"] in query_outputs:
                    rclone_sync_command = ["rclone", "sync", output_dict["file_storage_path"], os.getcwd()]
                    return_code = subprocess.call(rclone_sync_command)
                    output_name_pseudo = output_dict["original_file_name"].replace(".", "-")
                    fw_mod_spec["_push_all"][output_name_pseudo] = True
                    logger.info(
                        "{} queried from database with return code {}.".format(output_dict["original_file_name"],
                                                                               return_code))
        elif cal_cursor.count() == 0:
            logger.info(
                "For output files {}, with identifier {} and metaquery {}. No matching file could be found.".format(
                    query_outputs, output_labels, metaquery))

        if fw_mod_spec['_push_all']:
            return FWAction(mod_spec=[fw_mod_spec])

