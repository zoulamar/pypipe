#!/usr/bin/echo Script not meant as executable:

# Filename:  std_modules.py
# Author:    Ing. Martin Zoula (zoulamar@fel.cvut.cz)

from pypipe import BaseModule, GenericDataType
from pathlib import Path
from pypipe.datatypes import NpzDataType, YamlDataType
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from pypipe.sciplotrc import IEEE_COL_WIDTH
import os
import shutil
import numpy as np

class ConvertToNpz (BaseModule):
    """ Converts supported file formats into Numpy binary data. """

    def __init__(self, module_path: Path) -> None:
        super().__init__(module_path)

    def declare_targets(self) -> dict[str, GenericDataType]:
        config = YamlDataType(self.module_path / "config.yaml", GenericDataType.expect_made)
        ret = {}
        for t_name, t_obj in self.get_parent().targets.items():
            ret[t_name] = NpzDataType(Path(self.module_path / (t_obj.path.stem + ".npz")), self.maker, {"config": config, "src": t_obj})
        return ret

    @staticmethod
    def maker(target:NpzDataType):
        config_target:YamlDataType = target.depends["config"] # type:ignore
        config:dict = config_target.get()
        src_target = target.depends["src"]
        try:
            return {
                ".csv" : ConvertToNpz.maker_csv
            }[src_target.path.suffix](target, src_target, config)
        except KeyError as e:
            e.add_note(f"Input file extension {target.path.suffix} is not supported yet for conversion.")
            raise

    @staticmethod
    def maker_csv(target:NpzDataType, source:GenericDataType, config:dict):
        delim = "," if ("delimiter" not in config) else config["delimiter"]
        with open(source.path, "r") as f:
            labels = list(map(lambda x : x.strip(), f.readline().split(sep=delim)[1:]))
        comments = None if "comments" not in config else config["comments"]
        data = np.loadtxt(source.path, delimiter=delim, comments=comments)
        print(data, data.shape, len(labels))
        assert data.shape[1] == len(labels)
        ret = {}
        for i, label in enumerate(labels):
            ret[label] = data[:,i]
        target.set(ret)

class NpzDataset (BaseModule):
    """ Splits each NPZ source dataset in parent to learning, testing and validation data. Effectively reorganizes the numpy array, splitting "*" array into "*:tst", "*:val" and "*:trn"

    TODO? Enable some augmentations like gaussian noise addition or whatnot...
    """

    def __init__(self, module_path: Path) -> None:
        super().__init__(module_path)

    def declare_targets(self) -> dict[str, GenericDataType]:
        config = YamlDataType(self.module_path / "config.yaml", GenericDataType.expect_made)
        ret = {}
        for t_name, t_obj in self.get_parent().targets_by_type(NpzDataType):
            ret[t_name] = NpzDataType(Path(self.module_path / (t_obj.path.stem + ".npz")), self.maker, {"config": config, "src": t_obj})
        return ret

    @staticmethod
    def maker(target:NpzDataType):
        config_target:YamlDataType = target.depends["config"] # type:ignore
        config:dict = config_target.get()
        src_target = target.depends["src"]
        try:
            return {
                ".csv" : ConvertToNpz.maker_csv
            }[src_target.path.suffix](target, src_target, config)
        except KeyError as e:
            e.add_note(f"Input file extension {target.path.suffix} is not supported yet for conversion.")
            raise















