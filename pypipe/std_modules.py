#!/usr/bin/echo Script not meant as executable:

# Filename:  std_modules.py
# Author:    Ing. Martin Zoula (zoulamar@fel.cvut.cz)

from pypipe import BaseModule, GenericDataType
from pathlib import Path
from pypipe.std_datatypes import NpzDataType, YamlDataType
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

class RootModule (BaseModule):
    """ Just a generic class which blocks further recursive submodule search. """
    def __init__(self, module_path:Path, source_space:Path, verbose:bool) -> None:
        super().__init__(module_path, source_space, verbose, is_root_module=True)

class Plot (BaseModule):
    def __init__(self, module_path: Path, source_space: Path, verbose: bool, is_root_module: bool = False) -> None:
        super().__init__(module_path, source_space, verbose, is_root_module)
        assert self.parent_module is not None
        self.parent_module:BaseModule

    def declare_targets(self) -> dict[str, GenericDataType]:
        config_file = YamlDataType(self.module_path / "config.yaml", GenericDataType.expect_made)
        ret:dict[str, GenericDataType] = {}

        # Understand npy files
        for t_lab, t_obj in GenericDataType.filteritems(self.parent_module.targets,NpzDataType):
            ret[t_lab] = GenericDataType(self.module_path / t_obj.path.with_stem(".pdf").name, plot_npz, depends={"src":t_obj,"cfg":config_file})

        return ret

def plot_npz(target:GenericDataType):
    cfg = target.depends["cfg"].get()

    with PdfPages(target.path) as pdf:
        for data_name, data in target.depends["src"].get():
            plt.plot(data)


