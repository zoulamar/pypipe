#!/usr/bin/echo Script not meant as executable:

# Filename:  std_modules.py
# Author:    Ing. Martin Zoula (zoulamar@fel.cvut.cz)

from pypipe import BaseModule, GenericDataType
from pathlib import Path
from pypipe.std_datatypes import NpzDataType, YamlDataType
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from pypipe.sciplotrc import subplots, IEEE_COL_WIDTH
import os

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
            ret[t_lab] = GenericDataType(self.module_path / t_obj.path.with_suffix(".pdf").name, Plot.npz, depends={"src":t_obj,"cfg":config_file})

        return ret

    @staticmethod
    def npz(target:GenericDataType):
        cfg = target.depends["cfg"].get()
        src = target.depends["src"].get()
        print(src)
        try:
            with PdfPages(target.path) as pdf:
                for data_name, data in src.items():
                    fig = plt.figure(figsize=(IEEE_COL_WIDTH, IEEE_COL_WIDTH*.8))
                    plt.plot(data)
                    pdf.savefig()
                    plt.close()
        except:
            os.remove(target.path)
            raise
