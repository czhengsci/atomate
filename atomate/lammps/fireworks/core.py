# coding: utf-8

from __future__ import division, print_function, unicode_literals, absolute_import

"""
Defines fireworks to be incorporated into workflows.
"""

from pymatgen.io.lammps.topology import Topology

from fireworks import Firework

from atomate.common.firetasks.glue_tasks import PassCalcLocs
from atomate.lammps.firetasks.run_calc import RunLammpsDirect, RunPackmol
from atomate.lammps.firetasks.parse_outputs import LammpsToDB
from atomate.lammps.firetasks.write_inputs import WriteFromIOSet, WriteFromForceFieldAndTopology
from atomate.lammps.firetasks.glue_tasks import CopyPackmolOutputs

__author__ = "Brandon Wood, Kiran Mathew"
__email__ = "b.wood@berkeley.edu"


class LammpsFW(Firework):

    def __init__(self, lammps_input_set, input_filename="lammps.in", data_filename="lammps.data",
                 lammps_cmd="lammps", db_file=None, parents=None, name="LammpsFW",
                 log_filename="log.lammps", dump_filename=None, **kwargs):
        """
        write lammps inputset, run, and store the output.

        Args:
            lammps_input_set (DictLammpsInput): lammps input set
            input_filename (str): input file name
            data_filename (str): data file name
            lammps_cmd (str): command to run lammps (skip the input file).
                e.g. 'mpirun -n 8 lmp_mpi'
            db_file (str): path to file specifying db credentials to place output parsing.
            parents ([Fireworks)]: parents of this particular Firework.
            name (str): descriptive name for lammps simulation
            log_filename (str)
            dump_filename (str)
            \*\*kwargs: other kwargs that are passed to Firework.__init__.
        """

        tasks = [
            WriteFromIOSet(lammps_input_set=lammps_input_set, input_filename=input_filename,
                           data_filename=data_filename),

            RunLammpsDirect(lammps_cmd=lammps_cmd, input_filename=input_filename),

            LammpsToDB(input_filename=input_filename, data_filename=data_filename,
                       log_filename=log_filename, dump_filename=dump_filename,
                       db_file=db_file, additional_fields={"task_label": name})
        ]

        super(LammpsFW, self).__init__(tasks, parents=parents, name=name, **kwargs)


class LammpsForceFieldFW(Firework):

    def __init__(self, input_file, final_molecule, forcefield, box_size, topologies=None,
                 constituent_molecules=None, mols_number=None, user_settings=None,
                 ff_site_property=None, input_filename="lammps.in", data_filename="lammps.data",
                 lammps_cmd="lammps", db_file=None, parents=None, log_filename="log.lammps",
                 dump_filenames=None, name="LammpsFFFW", **kwargs):
        """

        Args:
            input_file (str): path to lammps input(or template) file.
            final_molecule (str/Molecule): either path to the moelcule of Molecule object.
            forcefield (ForceField): pymatgen.io.lammps.force_field.ForceField object
            box_size (list):  list of list of low and high values for each dimension [[xlow, xhigh], ...]
            topologies ([Topology]): list of pymatgen.io.lammps.topology.Topology objects, one for
                each constituent molecule.
            constituent_molecules ([Molecule]): list of Molecule objects that make up the final_molecule
            mols_number (list): list of number of each constituent moelcule.
            user_settings (dict):
            ff_site_property (str): the site property used for forcefiled mapping
            input_filename (str): name of the input file to be written
            data_filename (str):name of the data file to be written
            lammps_cmd (str): lammps command run (without the input file)
            db_file (str): path to the db settings
            parents (list): list of Fireworks
            log_filename (str): lammps log file name
            dump_filenames (str): list of dump files
            name (str): firework name
            **kwargs:
        """

        user_settings = user_settings or {}
        constituent_molecules = constituent_molecules or [final_molecule]
        mols_number = mols_number or [1]
        topologies = topologies or Topology.from_molecule(final_molecule, ff_map=ff_site_property)

        tasks = [
            CopyPackmolOutputs(calc_loc=True),

            WriteFromForceFieldAndTopology(input_file=input_file, final_molecule_path=final_molecule,
                                           constituent_molecules=constituent_molecules,
                                           mols_number=mols_number, forcefield=forcefield,
                                           topologies=topologies, input_filename=input_filename,
                                           user_settings=user_settings, ff_site_property=ff_site_property,
                                           box_size=box_size),

            RunLammpsDirect(lammps_cmd=lammps_cmd, input_filename=input_filename),

            LammpsToDB(input_filename=input_filename, data_filename=data_filename,
                       log_filename=log_filename, dump_filenames=dump_filenames,
                       db_file=db_file, additional_fields={"task_label": name})
        ]

        super(LammpsForceFieldFW, self).__init__(tasks, parents=parents, name=name, **kwargs)


class PackmolFW(Firework):

    def __init__(self, molecules, packing_config, tolerance=2.0, filetype="xyz", control_params=None,
                 output_file="packed.xyz",  copy_to_current_on_exit=False, site_property=None,
                 parents=None, name="PackmolFW", **kwargs):
        """

        Args:
            molecules:
            packing_config:
            tolerance:
            filetype:
            control_params:
            output_file:
            copy_to_current_on_exit:
            site_property:
            parents:
            name:
            **kwargs:
        """
        control_params = control_params or {'maxit': 20, 'nloop': 600}

        tasks = [
            RunPackmol(molecules=molecules, packing_config=packing_config, tolerance=tolerance,
                       filetype=filetype, control_params=control_params,  output_file=output_file,
                       copy_to_current_on_exit=copy_to_current_on_exit, site_property=site_property),

            PassCalcLocs(name=name)

             ]

        super(PackmolFW, self).__init__(tasks, parents=parents, name=name, **kwargs)
