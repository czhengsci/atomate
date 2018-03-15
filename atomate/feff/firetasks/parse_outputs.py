# coding: utf-8

from __future__ import division, print_function, unicode_literals, absolute_import

import json
import os
from datetime import datetime
from glob import glob
import six
import time
import logging
import re
from matgendb.util import get_database
import subprocess

import numpy as np

from pymatgen.io.feff.inputs import Tags, Atoms

from fireworks import FiretaskBase, FWAction, explicit_serialize
from fireworks.utilities.fw_serializers import DATETIME_HANDLER
from fireworks.user_objects.firetasks.filepad_tasks import get_fpad

from atomate.utils.utils import env_chk
from atomate.common.firetasks.glue_tasks import get_calc_loc
from atomate.utils.utils import get_logger
from atomate.feff.database import FeffCalcDb

__author__ = 'Kiran Mathew, Chen Zheng'
__email__ = 'kmathew@lbl.gov, chz022@ucsd.edu'

logging.basicConfig(filename="feff_run.log", level=logging.INFO)
logger = get_logger(__name__)


@explicit_serialize
class SpectrumToDbTask(FiretaskBase):
    """
    Parse the output of absorption/core-loss spectrum calculations(xmu.dat, eels.dat) and insert it
    into the database.

    Required_params:
        absorbing_atom (str): absorbing atom symbol
        structure (Structure): input structure
        spectrum_type (str): XANES, EXAFS, ELNES, EXELFS
        output_file (str): the output file name. xmu.dat or eels.dat

    Optional_params:
        input_file (str): path to the feff input file.
        calc_dir (str): path to dir (on current filesystem) that contains FEFF output files.
            Default: use current working directory.
        calc_loc (str OR bool): if True will set most recent calc_loc. If str search for the most
            recent calc_loc with the matching name
        db_file (str): path to the db file.
        edge (str): absorption edge
        metadata (dict): meta data
    """

    required_params = ["absorbing_atom", "structure", "spectrum_type", "output_file"]
    optional_params = ["input_file", "calc_dir", "calc_loc", "db_file", "edge", "metadata"]

    def run_task(self, fw_spec):
        calc_dir = os.getcwd()
        if "calc_dir" in self:
            calc_dir = self["calc_dir"]
        elif self.get("calc_loc"):
            calc_dir = get_calc_loc(self["calc_loc"], fw_spec["calc_locs"])["path"]

        logger.info("PARSING DIRECTORY: {}".format(calc_dir))

        db_file = env_chk(self.get('db_file'), fw_spec)

        cluster_dict = None
        tags = Tags.from_file(filename="feff.inp")
        if "RECIPROCAL" not in tags:
            cluster_dict = Atoms.cluster_from_file("feff.inp").as_dict()
        doc = {"input_parameters": tags.as_dict(),
               "cluster": cluster_dict,
               "structure": self["structure"].as_dict(),
               "absorbing_atom": self["absorbing_atom"],
               "spectrum_type": self["spectrum_type"],
               "spectrum": np.loadtxt(os.path.join(calc_dir, self["output_file"])).tolist(),
               "edge": self.get("edge", None),
               "metadata": self.get("metadata", None),
               "dir_name": os.path.abspath(os.getcwd()),
               "last_updated": datetime.utcnow()}

        if not db_file:
            with open("feff_task.json", "w") as f:
                f.write(json.dumps(doc, default=DATETIME_HANDLER))

        else:
            db = FeffCalcDb.from_db_file(db_file, admin=True)
            db.insert(doc)

        logger.info("Finished parsing the spectrum")

        return FWAction(stored_data={"task_id": doc.get("task_id", None)})


@explicit_serialize
class AddPathsToFilepadTask(FiretaskBase):
    """
    Insert the scattering amplitude outputs(all feffNNNN.dat files) to gridfs using filepad.

    Optional_params:
        labels (list): list of labels to tag the inserted files. Useful for querying later.
        filepad_file (str): path to the filepad connection settings file.
        compress (bool): whether or not to compress the file contents before insertion.
        metadata (dict): metadata.
    """

    optional_params = ["labels", "filepad_file", "compress", "metadata"]

    def run_task(self, fw_spec):
        paths = glob("feff????.dat")
        fpad = get_fpad(self.get("filepad_file", None))
        labels = self.get("labels", None)
        for i, p in enumerate(paths):
            l = labels[i] if labels is not None else None
            fpad.add_file(p, l, metadata=self.get("metadata", None),
                          compress=self.get("compress", True))


@explicit_serialize
class AddPathsToStorageTask(FiretaskBase):
    """
    Insert the module outputs (default to be pot.bin, phase.bin, xsect.dat) to storage system using rclone
    Required_params:
        output_identifier (str/list): Identifier label(s) to tag the inserted module output files. Useful
                                    for querying later.
        index_collection (str): Name of the collection in Mongodb stores information, i.e calculation parameters,
                                file pathways, etc., of calculated intermediate file..
        indexdb_settings (str): File path to json file used for index database connection setup

    Optional_params:
        metadata (dict): metadata.
        rclone_remote (str): The remote name used in rclone sync command
        rclone_destpath (str): The destination path rclone will sync to.
        calc_dir (str): path to dir (on current filesystem) that contains FEFF output files.
                        Default: use current working directory.
        calc_loc (str OR bool): if True will set most recent calc_loc. If str search for the most
                        recent calc_loc with the matching name
    """
    required_params = ["output_identifier", "index_collection",
                       "indexdb_settings"]
    optional_params = ["metadata", "rclone_remote", "rclone_destpath", "calc_dir", "calc_loc"]

    def run_task(self, fw_spec):
        calc_dir = os.getcwd()
        if "calc_dir" in self:
            calc_dir = self["calc_dir"]
        elif self.get("calc_loc"):
            calc_dir = get_calc_loc(self["calc_loc"], fw_spec["calc_locs"])["path"]

        logger.info("PARSING DIRECTORY: {}".format(calc_dir))

        module_outputs = self["module_outputs"]
        rclone_remote_name = self["rclone_remote"]
        rclone_destpath = self["rclone_destpath"]
        metadata = self.get("metadata", dict())

        if isinstance(self["output_identifier"], six.string_types):
            labels = self["output_identifier"]
        elif isinstance(self["output_identifier"], (list,)):
            labels = '-'.join(self["output_identifier"])

        tags = Tags.from_file(glob(os.path.join(calc_dir, "feff.inp"))[0])
        metadata["input_parameters"] = tags.as_dict()

        index_db_connection = get_database(self["indexdb_settings"], admin=True)

        # Searching indexdb using the labels and input_parameters of metadata.
        identifier_pattern = re.compile("{}.*".format(labels))
        cal_cursor = index_db_connection[self["index_collection"]].find({"identifier": identifier_pattern,
                                                                         "metadata.input_parameters": metadata[
                                                                             "input_parameters"]})
        paths = glob("feff????.dat")

        if cal_cursor.count() == 0:
            timestr = time.strftime("%Y%m%d-%H%M%S")
            output_subdict = dict()
            output_subdict["metadata"] = metadata
            output_subdict["identifier"] = "-".join((labels, timestr))
            remote_storage_folder = ":".join((rclone_remote_name, rclone_destpath)) + "/" + output_subdict[
                "identifier"]

            # rclone and store individual FEFF path files
            output_subdict["feffpaths"] = []
            for path in paths:
                subpath_dict = dict()
                subpath_dict["file_path"] = glob(os.path.join(calc_dir, path))[0]
                subpath_dict["original_file_name"] = os.path.basename(subpath_dict["file_path"])
                subpath_dict["file_storage_path"] = "/".joins(
                    (remote_storage_folder, subpath_dict["original_file_name"]))
                rclone_sync_command = ["rclone", "sync", subpath_dict["file_path"],
                                       subpath_dict["file_storage_path"]]
                return_code = subprocess.call(rclone_sync_command)

                if return_code != 0:
                    logger.info("{} path uploading failed with return code {}.".format(path, return_code))
                    if not ("feff_failed_upload" in output_subdict):
                        output_subdict["feff_failed_upload"] = []
                    output_subdict["feff_failed_upload"].append(subpath_dict)
                elif return_code == 0:
                    output_subdict["feffpaths"].append(subpath_dict)

            index_db_connection[self["index_collection"]].update_one({"identifier": output_subdict["identifier"]},
                                                                     {"$set": output_subdict}, upsert=True)

        elif cal_cursor.count() > 0:
            existed_entry = cal_cursor.next()
            inter_files = existed_entry["intermediate_files"]
            storage_dir_paths = []
            for _sub in inter_files:
                storage_dir_paths.append(_sub["file_storage_path"].rstrip('/' + _sub["original_file_name"]))

            if len(np.unique(storage_dir_paths)) == 1:
                remote_storage_folder = storage_dir_paths[0]
                feffpath_field = []
                for path in paths:
                    subpath_dict = dict()
                    subpath_dict["file_path"] = glob(os.path.join(calc_dir, path))[0]
                    subpath_dict["original_file_name"] = os.path.basename(subpath_dict["file_path"])
                    subpath_dict["file_storage_path"] = "/".join((remote_storage_folder,
                                                                  subpath_dict["original_file_name"]))
                    rclone_sync_command = ["rclone", "sync", subpath_dict["file_path"],
                                           subpath_dict["file_storage_path"]]
                    return_code = subprocess.call(rclone_sync_command)
                    if return_code != 0:
                        logger.info("{} path uploading failed with return code {}.".format(path, return_code))
                        if not ("feff_failed_upload" in existed_entry):
                            existed_entry["feff_failed_upload"] = []
                            existed_entry["feff_failed_upload"].append(subpath_dict)
                    elif return_code == 0:
                        feffpath_field.append(subpath_dict)

                index_db_connection[self["index_collection"]].update_one({"identifier": identifier_pattern,
                                                                          "metadata.input_parameters": metadata[
                                                                              "input_parameters"]},
                                                                         {"$set": {"feffpaths": feffpath_field}})
                if "feff_failed_upload" in existed_entry:
                    index_db_connection[self["index_collection"]].update_one({"identifier": identifier_pattern,
                                                                              "metadata.input_parameters": metadata[
                                                                                  "input_parameters"]},
                                                                             {"$set": {
                                                                                 "feff_failed_upload": existed_entry[
                                                                                     "feff_failed_upload"]}})

            else:
                raise ValueError("Intermediate files's storage paths are inconsistent.")


@explicit_serialize
class AddModuleOutputsToStorageTask(FiretaskBase):
    """
    Insert the module outputs (default to be pot.bin, phase.bin, xsect.dat) to storage system using rclone
    Required_params:
        module_outputs (list): List of output intermediate files need to be kept in cloud storage.
        output_identifier (str/list): Identifier label(s) to tag the inserted module output files. Useful
                                    for querying later.
        rclone_remote (str): The remote name used in rclone sync command
        rclone_destpath (str): The destination path rclone will sync to.
        index_collection (str): Name of the collection in Mongodb stores information, i.e calculation parameters,
                                file pathways, etc., of calculated intermediate file..
        indexdb_settings (str): File path to json file used for index database connection setup

    Optional_params:
        metadata (dict): metadata.
        calc_dir (str): path to dir (on current filesystem) that contains FEFF output files.
                        Default: use current working directory.
        calc_loc (str OR bool): if True will set most recent calc_loc. If str search for the most
                        recent calc_loc with the matching name
    """
    required_params = ["module_outputs", "output_identifier", "rclone_remote", "rclone_destpath", "index_collection",
                       "indexdb_settings"]
    optional_params = ["metadata", "calc_dir", "calc_loc"]

    def run_task(self, fw_spec):
        calc_dir = os.getcwd()
        if "calc_dir" in self:
            calc_dir = self["calc_dir"]
        elif self.get("calc_loc"):
            calc_dir = get_calc_loc(self["calc_loc"], fw_spec["calc_locs"])["path"]

        logger.info("PARSING DIRECTORY: {}".format(calc_dir))

        module_outputs = self["module_outputs"]
        rclone_remote_name = self["rclone_remote"]
        rclone_destpath = self["rclone_destpath"]
        metadata = self.get("metadata", dict())

        if isinstance(self["output_identifier"], six.string_types):
            labels = self["output_identifier"]
        elif isinstance(self["output_identifier"], (list,)):
            labels = '-'.join(self["output_identifier"])

        tags = Tags.from_file(glob(os.path.join(calc_dir, "feff.inp"))[0])
        metadata["input_parameters"] = tags.as_dict()

        # Need to check whether the FEFF run's SCF calculation converge. If not, no need for intermediate
        # file storage
        converge_pattern = re.compile('Convergence reached.*')
        not_converge_pattern = re.compile('Convergence not reached.*')
        log1data_path = glob(os.path.join(calc_dir, "log1.dat"))[0]
        scf_converged = False
        for i, line in enumerate(open(log1data_path)):
            if len(not_converge_pattern.findall(line)) > 0:
                scf_converged = False
                break
            if len(converge_pattern.findall(line)) > 0:
                scf_converged = True

        if scf_converged:
            index_db_connection = get_database(self["indexdb_settings"], admin=True)

            # Searching indexdb using the labels and input_parameters of metadata.
            identifier_pattern = re.compile("{}.*".format(labels))
            cal_cursor = index_db_connection[self["index_collection"]].find({"identifier": identifier_pattern,
                                                                             "metadata.input_parameters": metadata[
                                                                                 "input_parameters"]})

            if cal_cursor.count() == 0:
                timestr = time.strftime("%Y%m%d-%H%M%S")
                output_subdict = dict()
                output_subdict["metadata"] = metadata
                output_subdict["identifier"] = "-".join((labels, timestr))
                remote_storage_folder = ":".join((rclone_remote_name, rclone_destpath)) + "/" + output_subdict[
                    "identifier"]

                #Storage feff.inp files
                output_subdict["feffinput"] = dict()
                output_subdict["feffinput"]["file_path"] = glob(os.path.join(calc_dir, "feff.inp"))[0]
                output_subdict["feffinput"]["original_file_name"] = "feff.inp"
                output_subdict["feffinput"]["feffinput_storage_path"] =  "/".join(
                        (remote_storage_folder, "feff.inp"))
                rclone_sync_command = ["rclone", "sync", output_subdict["feffinput"]["file_path"],
                                       output_subdict["file_storage_path"]]
                return_code = subprocess.call(rclone_sync_command)
                logger.info("Stored feff.inp file: {}. Storage path: {}. Return code: {}".format(
                    output_subdict["original_file_name"],
                    output_subdict["file_storage_path"], return_code))

                #Storage dos files is LDOS in feff.inp
                if "LDOS" in tags:
                    output_subdict["ldos"] = dict()
                    output_subdict["ldos"]["emin"] = tags["ldos"][0]
                    output_subdict["ldos"]["emax"] = tags["ldos"][1]
                    output_subdict["ldos"]["eimag"] = tags["ldos"][2]
                    output_subdict["ldos"]["dos_files"] = []
                    ldos_dat = glob(os.path.join(calc_dir, "ldos??.dat"))
                    for ldos in ldos_dat:
                        subdos_dict = dict()
                        subdos_dict["file_path"] = ldos
                        subdos_dict["original_file_name"] = os.path.basename(ldos)
                        subdos_dict["file_storage_path"] = "/".joins(
                            (remote_storage_folder, subdos_dict["original_file_name"]))
                        rclone_sync_command = ["rclone", "sync", subdos_dict["file_path"],
                                               subdos_dict["file_storage_path"]]
                        return_code = subprocess.call(rclone_sync_command)
                        output_subdict["ldos`"]["dos_files"].append(subdos_dict)


                # rclone and store individual intermediate files
                output_subdict["intermediate_files"] = []
                for _, output in enumerate(module_outputs):
                    inter_subdict = dict()
                    inter_subdict["file_path"] = glob(os.path.join(calc_dir, output))[0]
                    inter_subdict["original_file_name"] = os.path.basename(inter_subdict["file_path"])
                    inter_subdict["file_storage_path"] = "/".join(
                        (remote_storage_folder, inter_subdict["original_file_name"]))

                    rclone_sync_command = ["rclone", "sync", inter_subdict["file_path"],
                                           inter_subdict["file_storage_path"]]
                    return_code = subprocess.call(rclone_sync_command)
                    logger.info("Stored intermediate file: {}. Storage path: {}. Return code: {}".format(
                        inter_subdict["original_file_name"],
                        inter_subdict["file_storage_path"], return_code))

                    output_subdict["intermediate_files"].append(inter_subdict)

                index_db_connection[self["index_collection"]].update_one({"identifier": output_subdict["identifier"]},
                                                                         {"$set": output_subdict}, upsert=True)
            elif cal_cursor.count() > 0:
                logger.info(
                    "Intermediate file of {} already existed in the database. Calculation input parameters are {}".format(
                        labels, metadata["input_parameters"]))

        elif not scf_converged:
            logger.info("The SCF calculation of FEFF run does not converge.")
