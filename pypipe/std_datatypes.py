#!/usr/bin/echo Script not meant as executable:

# Filename:  std_modules.py
# Date:      2022-11
# Author:    Ing. Martin Zoula (zoulamar@fel.cvut.cz)

from pypipe import GenericDataType
from yaml import safe_load, safe_dump
from pprint import pprint, pformat
import numpy as np

class YamlDataType(GenericDataType):
    """ TODO: Add a method which would check some intended structure and types in the file. """
    def load(self):
        self.value = safe_load(open(str(self.path), "r").read())

    def save(self):
        with open(str(self.path), "w") as f:
            safe_dump(self.value, f)

    def str_detailed(self):
        loaded = self.load()
        pprint(loaded)

class NpzDataType(GenericDataType):
    """ Numpy-centric storage of several named arrays in a zip archive. """
    def load(self):
        self.value = dict(np.load(self.path, allow_pickle=True))

    def save(self):
        assert self.value is not None
        print(f"Saving value {self.value} to {self.path}")
        np.savez_compressed(self.path, **self.value)

    def str_detailed(self):
        try:
            loaded = self.get()
            assert isinstance(loaded, dict)
            out = ""
            for label, data in loaded.items():
                out += f"Array '{label}' of shape {data.shape}:\n"
                out += pformat(data)
            return out
        except FileNotFoundError:
            return f"File not ready yet."

    def plot(self, into):
        raise NotImplementedError()

