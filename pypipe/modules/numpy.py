#!/usr/bin/echo Script not meant as executable:

# Filename:  std_modules.py
# Author:    Ing. Martin Zoula (zoulamar@fel.cvut.cz)

from pypipe import BaseModule, GenericDataType
from pathlib import Path
from pypipe.datatypes import NpzDataType, YamlDataType
import numpy as np
from collections import defaultdict

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
    """ Annotates each NPZ source dataset in parent with training, validation and testing data. Effectively reorganizes the numpy array, splitting "*" array into "*:tst", "*:val" and "*:trn"

    TODO? Enable some augmentations like gaussian noise addition or whatnot...
    """

    def __init__(self, module_path: Path) -> None:
        super().__init__(module_path)

    def declare_targets(self) -> dict[str, GenericDataType]:
        config = YamlDataType(self.module_path / "config.yaml", GenericDataType.expect_made)
        config_data = config.get()
        xvalin = NpzDataset.cfg_xval(config_data)
        if "random-seed" in config_data:
            np.random.seed(config_data["random-seed"])
        ret = {}
        for t_name, t_obj in self.get_parent().targets_by_type(NpzDataType):
            if xvalin in (None, False,  0):
                ret[t_name] = NpzDataType(Path(self.module_path / (t_name + ".npz")), self.maker, {"config": config, "src": t_obj})
            else:
                assert isinstance(xvalin, int)
                pad=int(np.log10(xvalin))+1
                for i in range(xvalin):
                    t_name_i = f"{t_name}:xval{i:0{pad}d}"
                    ret[t_name_i] = NpzDataType(Path(self.module_path / (t_name_i + ".npz")), self.maker, {"config": config, "src": t_obj})
        return ret

    @staticmethod
    def cfg_xval(cfg):
        if "cross-validation-instances" in cfg:
            xvalin = cfg["cross-validation-instances"]
        else:
            xvalin = None
        return xvalin

    @staticmethod
    def maker(target:NpzDataType):
        """"""

        # Load inputs.
        config_target:YamlDataType = target.depends["config"] # type:ignore
        src_target:NpzDataType = target.depends["src"] # type:ignore
        config = config_target.get()
        data:dict[str,np.ndarray] = src_target.get()
        try:
            rows,_ = src_target.to_array().shape # NOTE: If input data is of non-homogeneous size, this will fail.
        except:
            raise ValueError("Source data is not homogeneous.")

        # Prepare return
        ret = dict()

        # If cross-validation scheme required, prepare it here.
        xvalin = NpzDataset.cfg_xval(config)
        if isinstance(xvalin, int) and xvalin > 0:
            # Compute how many samples per cross-validation class.
            ratios = np.array([float(config[x]) for x in ("trn", "val", "tst")])
            assert np.sum(ratios) <= 1, f"Training, validation and testing proportions sum to over 1. ({ratios})"
            n = np.floor(ratios * rows)

            # Assign labels to each dataset row.
            labels = np.ones((rows,1)) * -1 # Default class -1 = unassigned.
            labels[:int(n[0])] = 0
            labels[int(n[0]):int(n[0]+n[1])] = 1
            labels[int(n[0]+n[1]):int(n[0]+n[1]+n[2])] = 2 # NOTE: Here a sample may be omitted due to rounding.if config["shuffle"]:
            if "shuffle" in config and config["shuffle"] is True:
                np.random.shuffle(labels)
            hist = defaultdict(int)
            for i in labels.squeeze():
                hist[i] += 1
            print(hist)

            # Add.
            ret["xval_labels"] = labels

        # Merge selected input vectors into X and Y regresison feature vectors.
        for XY in ("X", "Y"):
            vec = []
            for descr in config[f"{XY}_fields"]:
                if descr.startswith("="):
                    raise NotImplementedError("Tbd.")
                else:
                    vec.append(data[descr])
            ret[XY] = np.array(vec).T

        # Copy misceleneaous fields
        if "misc_fields" in config:
            for f in config["misc_fields"]:
                ret[f] = data[f]

        print(ret)
        target.set(ret)




















