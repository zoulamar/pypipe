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
import shutil

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

        if "t" in src:
            t = src["t"]
            t = t - t[0]
            del src["t"]
        elif "time" in src:
            t = src["time"]
            t = t - t[0]
            del src["time"]
        else:
            t = None

        try:
            with PdfPages(target.path) as pdf:
                for data_name, data in src.items():
                    fig = plt.figure(figsize=(IEEE_COL_WIDTH, IEEE_COL_WIDTH*.8))
                    if t is None:
                        plt.plot(data)
                        plt.xlabel("Sample [-]")
                    else:
                        plt.plot(t, data)
                        plt.xlabel("Time [s]")
                    plt.ylabel(data_name)
                    pdf.savefig()
                    plt.close()
        except:
            os.remove(target.path)
            raise

class RemoteData (RootModule):
    def __init__(self, module_path: Path, source_space: Path, verbose: bool) -> None:
        super().__init__(module_path, source_space, verbose)

    def declare_targets(self) -> dict[str, GenericDataType]:
        config = YamlDataType(self.module_path / "config.yaml", GenericDataType.expect_made)
        ret = {}

        # This represents a meta-target which factually copies the remote files.
        targets = YamlDataType(self.module_path / "targets.yaml", self.refresh_targets, depends={"config": config})
        ret["__targets__"] = targets

        # If the target is already made, which must be done previously, a list of main targets may be declared.
        if targets.path.exists():
            for datafile_path in map(Path, targets.get()):
                ret[datafile_path.stem] = GenericDataType(datafile_path, GenericDataType.expect_made, {})
        return ret

    def refresh_targets (self, target:YamlDataType):
        """ Finds, copies and links the targets. """
        config = target.depends["config"].get()

        # Detect all remote files to be copied / cached. No problem if
        globs = [config["path"]] if "path" in config else config["paths"]
        assert isinstance(globs, list)
        missing_is_ok = config["mounted"]
        paths = []
        for glob in globs:
            if glob.startswith("/"):
                glob = glob[1:]
            result = list(Path("/").glob(glob))
            if len(result) == 0 and not missing_is_ok:
                raise FileNotFoundError(glob)
            for match in result:
                if match.exists():
                    paths.append(match)
                elif not missing_is_ok:
                    raise FileNotFoundError(glob)

        target_dir = target.path.parent
        result_paths = []
        touch_paths = []
        for src_path in paths:
            assert isinstance(src_path, Path)
            dst_path = target_dir / src_path.name
            touch_paths.append(dst_path)
            result_paths.append(str(shutil.copy2(src_path, dst_path)))
        target.set(result_paths)

        for dst_path in touch_paths:
            dst_path.touch()
