# coding: utf-8

from __future__ import division, print_function, unicode_literals, absolute_import

"""
This module defines tasks for writing FEFF input sets.
"""

import os
from six import string_types

from pymatgen.io.feff.inputs import Paths, Tags

from fireworks import FiretaskBase, explicit_serialize

from atomate.utils.utils import load_class

__author__ = 'Kiran Mathew, Chen Zheng'
__email__ = 'kmathew@lbl.gov, chz022@ucsd.edu'


@explicit_serialize
class WriteFeffFromIOSet(FiretaskBase):
    """
    Generate FEFF input (feff.inp) from the given InputSet object or InputSet name

    Required_params:
        absorbing_atom (str): absorbing atom symbol
        structure (Structure): input structure
        feff_input_set (str or FeffDictSet subclass): The inputset for setting params. If string
            then either the entire path to the class or the spectrum type must be provided
            e.g. "pymatgen.io.feff.sets.MPXANESSet" or "XANES"

    Optional_params:
        radius (float): cluster radius in angstroms
        other_params (dict): **kwargs to pass into the desired InputSet if using str feff_input_set
    """
    required_params = ["absorbing_atom", "structure", "feff_input_set"]
    optional_params = ["radius", "other_params"]

    def run_task(self, fw_spec):
        feff_input_set = get_feff_input_set_obj(self["feff_input_set"], self["absorbing_atom"],
                                                self["structure"], self.get("radius", 10.0),
                                                **self.get("other_params", {}))
        feff_input_set.write_input(".")


@explicit_serialize
class WriteEXAFSPaths(FiretaskBase):
    """
    Write the scattering paths to paths.dat file.

    Required_params:
        feff_input_set: (FeffDictSet subclass)
        paths (list): list of paths. A path = list of site indices.

    Optional_params:
        degeneracies (list): list of path degeneracies.
    """
    required_params = ["feff_input_set", "paths"]
    optional_params = ["degeneracies"]

    def run_task(self, fw_spec):
        atoms = self['feff_input_set'].atoms
        paths = Paths(atoms, self["paths"], degeneracies=self.get("degeneracies", []))
        paths.write_file()


@explicit_serialize
class WriteXASIOSetFromPrev(FiretaskBase):
    required_params = ["prev_calc_dir", "absorbing_atom", "structure", "xas_type"]
    optional_params = ["radius", "other_params"]

    def run_task(self, fw_spec):
        prev_calc_dir = os.path.abspath(self["prev_calc_dir"])
        d = dict(Tags.from_file(os.path.join(prev_calc_dir, "feff.inp")))
        d["CONTROL"] = ["0 0 0 0 1 1"]
        tags_to_delete = ["LDOS"]
        xas_type = self["xas_type"]
        for x in ["ELNES", "EXELFS", "XANES", "EXAFS"]:
            if x in d:
                d[xas_type] = d[x]["ENERGY"] if "ENERGY" in d[x] else d[x]
                tags_to_delete.append(x)
                break
        d["_del"] = tags_to_delete
        other_params = self.get("other_params", {})
        user_tag_settings = other_params.get("user_tag_settings", {}) or {}
        user_tag_settings.update(d)
        other_params["user_tag_settings"] = user_tag_settings
        feff_input_set = get_feff_input_set_obj(self["xas_type"], self["absorbing_atom"],
                                                self["structure"], self.get("radius", 10.0),
                                                **other_params)
        feff_input_set.write_input(".")


@explicit_serialize
class WriteXASOutputTemp(FiretaskBase):
    """
    This is a temporary Firetask class used for generation of FEFF input (feff.inp)
    from the given InputSet object or InputSet name. In the first step, it will check whether
    previous output files existing or not. If previous output files exist, the CONTROL card of
    feff_input_set will be change accordingly.

    Required_params:
        absorbing_atom (str): absorbing atom symbol
        structure (Structure): input structure
        feff_input_set (str or FeffDictSet subclass): The inputset for setting params. If string
            then either the entire path to the class or the spectrum type must be provided
            e.g. "pymatgen.io.feff.sets.MPXANESSet" or "XANES"

    Optional_params:
        radius (float): cluster radius in angstroms
        other_params (dict): **kwargs to pass into the desired InputSet if using str feff_input_set
    """
    required_params = ["absorbing_atom", "structure", "feff_input_set"]
    optional_params = ["radius", "other_params"]

    def run_task(self, fw_spec):
        feff_input_set = get_feff_input_set_obj(self["feff_input_set"], absorbing_atom=self["absorbing_atom"],
                                                structure=self["structure"], radius=self.get("radius", 10.0),
                                                user_tag_settings=self.get("other_params", {}))
        current_dir = os.getcwd()

        if os.path.isfile(os.path.join(current_dir, 'pot.bin')):
            ori_control_card = feff_input_set.config_dict["CONTROL"].split(" ")
            ori_control_card[0] = "0"
            feff_input_set.config_dict["CONTROL"] = " ".join(ori_control_card)

        feff_input_set.write_input(".")


def get_feff_input_set_obj(fis, *args, **kwargs):
    """
    returns feff input set object.

    Args:
        fis (str or FeffDictSet subclass): The inputset for setting params. If string then
            the entire path to the class or the spectrum type must be provided
            e.g. "pymatgen.io.feff.sets.MPXANESSet" or "XANES"
        args (tuple): feff input set args
        kwargs (dict): feff input set kwargs

    Returns:
        FeffDictSet object
    """
    # e.g. "pymatgen.io.feff.sets.MPXANESSet" or "XANES"
    if isinstance(fis, string_types):
        fis_ = "pymatgen.io.feff.sets.MP{}Set".format(fis) if "pymatgen" not in fis else fis
        modname, classname = fis_.strip().rsplit(".", 1)
        fis_cls = load_class(modname, classname)
        return fis_cls(*args, **kwargs)
    else:
        return fis
