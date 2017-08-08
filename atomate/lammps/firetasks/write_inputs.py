# coding: utf-8

from __future__ import division, print_function, unicode_literals, absolute_import

"""
This module defines firetasks for writing LAMMPS input files (data file and the control 
parameters file)
"""

import six

from pymatgen import Molecule
from pymatgen.io.lammps.force_field import ForceField
from pymatgen.io.lammps.data import LammpsForceFieldData
from pymatgen.io.lammps.sets import LammpsInputSet

from fireworks import FiretaskBase, explicit_serialize

__author__ = 'Kiran Mathew, Brandon Wood'
__email__ = "kmathew@lbl.gov, b.wood@berkeley.edu"


@explicit_serialize
class WriteFromIOSet(FiretaskBase):
    """
    Writes LAMMPS Input files(data file and the control parameters file) from DictLammpsInput.

    required_params:
        lammps_input_set (LammpsInputSet)
        input_file (string): name of the file to which the input params will be written

    optional_params:
        data_filename (string): if specified the data file will be renamed
    """

    required_params = ["lammps_input_set", "input_filename"]

    optional_params = ["data_filename"]

    def run_task(self, fw_spec):

        lammps_input_set = self["lammps_input_set"]
        input_filename = self["input_filename"]
        data_filename = self.get("data_filename", None)

        lammps_input_set.write_input(input_filename, data_filename)


@explicit_serialize
class WriteFromForceFieldAndTopology(FiretaskBase):

    required_params = ["input_file", "final_molecule", "constituent_molecules", "mols_number",
                       "box_size" "forcefield", "topologies", "input_filename"]

    optional_params = ["user_settings", "ff_site_property"]

    def run_task(self, fw_spec):

        molecules = self["constituent_molecules"]
        mols_number = self["mols_number"]
        input_filename = self["input_filename"]
        forcefield = self["forcefield"]
        topologies = self["topologies"]

        user_settings = self.get("user_settings", {})
        data_filename = user_settings.get("data_file", "lammps.data")
        final_molecule = self["final_molecule"]
        if isinstance(final_molecule, six.string_types):
            final_molecule = Molecule.from_file(final_molecule)

        lammps_ff_data = LammpsForceFieldData.from_forcefield_and_topology(molecules, mols_number,
                                                                           self["box_size"],
                                                                           final_molecule,
                                                                           forcefield, topologies)

        lammps_input_set = LammpsInputSet.from_file("forcefield", self["input_file"],
                                                    user_settings=user_settings,
                                                    lammps_data=lammps_ff_data,
                                                    data_filename=data_filename,
                                                    is_forcefield=True)

        lammps_input_set.write_input(input_filename, data_filename)
