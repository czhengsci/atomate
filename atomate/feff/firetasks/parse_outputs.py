# coding: utf-8

from __future__ import division, print_function, unicode_literals, absolute_import

import json
import os
from datetime import datetime
from glob import glob
import six

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
               "last_updated": datetime.today()}

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
class AddModuleOutputsToFilepadTask(FiretaskBase):
    """
    Insert the module outputs (default to be pot.bin, phase.bin, xsect.dat) to gridfs and filepad
    Required_params:
        module_outputs (list): List of output files insert into gridfs using filepad
        output_identifier (str/list): Identifier label(s) to tag the inserted module output files. Useful
                                    for querying later.

    Optional_params:

        filepad_file (str): path to the filepad connection settings file.
        compress (bool): whether or not to compress the file contents before insertion.
        calc_dir (str): path to dir (on current filesystem) that contains FEFF output files.
                        Default: use current working directory.
        calc_loc (str OR bool): if True will set most recent calc_loc. If str search for the most
                        recent calc_loc with the matching name
        metadata (dict): metadata.
    """
    required_params = ["module_outputs", "output_identifier"]
    optional_params = ["filepad_file", "compress", "metadata", "calc_dir", "calc_loc"]

    def run_task(self, fw_spec):
        calc_dir = os.getcwd()
        if "calc_dir" in self:
            calc_dir = self["calc_dir"]
        elif self.get("calc_loc"):
            calc_dir = get_calc_loc(self["calc_loc"], fw_spec["calc_locs"])["path"]

        logger.info("PARSING DIRECTORY: {}".format(calc_dir))

        module_outputs = self["module_outputs"]
        labels = self["output_identifier"]
        metadata = self.get("metadata", dict())

        tags = Tags.from_file(glob(os.path.join(calc_dir, "feff.inp"))[0])
        metadata["input_parameters"] = tags.as_dict()
        # metadata = {**metadata, **tags}
        outputs_list = []
        for index, output in enumerate(module_outputs):
            output_subdict = dict()
            output_subdict["file_path"] = glob(os.path.join(calc_dir, output))[0]
            output_subdict["original_file_name"] = os.path.basename(output_subdict["file_path"])

            if isinstance(labels, six.string_types):
                output_subdict["file_label"] = '-'.join((labels, output_subdict["original_file_name"]))
            elif isinstance(labels, (list,)):
                output_subdict["file_label"] = labels[index]
            outputs_list.append(output_subdict)

        fpad = get_fpad(self.get("filepad_file", None))
        for output_file in outputs_list:
            insert_file_path = output_file["file_path"]
            insert_file_label = output_file["file_label"]
            fpad.add_file(insert_file_path, insert_file_label, metadata=metadata,
                          compress=self.get("compress", True))
