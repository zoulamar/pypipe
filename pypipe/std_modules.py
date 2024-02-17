#!/usr/bin/echo Script not meant as executable:

# Filename:  std_modules.py
# Date:      2022-11
# Author:    Ing. Martin Zoula (zoulamar@fel.cvut.cz)

from pypipe import BaseModule
from pathlib import Path

class RootModule (BaseModule):
    """ Just a generic class which blocks further recursive submodule search. """
    def __init__(self, module_path:Path, source_space:Path, verbose:bool) -> None:
        super().__init__(module_path, source_space, verbose, is_root_module=True)

class Plot (BaseModule):
    ...
