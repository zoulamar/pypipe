#!/usr/bin/echo Script not meant as executable:

import sys
from abc import ABC, abstractmethod
import os, time
from pathlib import Path
from typing import Callable, Dict, Iterator, Literal, Tuple, Type, Union, Any, final
from colored import fg, attr
from pprint import pformat

class GenericDataType(ABC):
    """ An abstraction of a file at specific path with associated dependencies and a maker function.

    Features:
        - Consistent filesystem unit abstraction with strict Python semantics. (Python data-oriented object with its 1-to-1 filesystem counterpart. Plus extra sweet stuff like methods etc.)
        - Dependencies between targets. (Recursive up-to-dateness evaluation, minimal update detection, ...)
        - A method reference to factually make this specified target. Such method needs to have correct input targets defined.
        - Standard saving and loading wrapper.
        - File-independent data exchange format. I.e., allows passing data in-memory.

    No-features:
        - Production mode management. (When you deploy a neural network, some targets like a learned parameters are not desired for recomputation. Use some root module's property as a flag for this purpose.)
    """

    USED_PATH_REGISTRY:Dict[Path,"GenericDataType"] = {}
    """ This is to ensure that one file does not accidentaly have two different coexisting Target objects or two instances of the same target loaded in memory. """

    AUTO_SAVING_TO_FILE = True
    """ Set this to False to globally disable saving the results to file whenever `set` is called. Useful when deploying the pipeline on robot. """

    @staticmethod
    def filteritems(d:dict,t:Type)->Iterator[Tuple[str,"GenericDataType"]]:
        """ Generates tuple `name, value` only if `value` has same type as self. """
        for n, v in d.items():
            if isinstance(v, t):
                yield n,v

    @staticmethod
    def nop(*_, **__) -> Literal[True]:
        """ A placeholder callable `maker` used when the target is not made via this system. """
        return True

    def __init__(self, where:Path, maker:Callable, depends:Dict[str,"GenericDataType"] = {}, params:Any=None, parallelizable:str="100%") -> None:
        """ The `GenericDataType` has to be instantiated with some particular filesystem path which may be used for persistent storage of this target's value.

        However, the target can be also passed some runtime data via `self.set(...)`, bypassing those stored on `where`.
        This might be useful when the Pipeline is already trained and used only for computing the predictions.
        """

        assert where.resolve() not in GenericDataType.USED_PATH_REGISTRY, f"Module at path {where} already present:\n{pformat(GenericDataType.USED_PATH_REGISTRY)}"
        GenericDataType.USED_PATH_REGISTRY[where.resolve()] = self

        self.path:Path = where
        """ A filesystme path where this target resides. """

        self.depends:Dict[str,"GenericDataType"] = depends
        """ A dict of named references enumerating all other data units required for computation. """

        self.params:Any = params
        """ A possible object of any type which influences the target's computation in some way. """

        self.parallelizable:str = parallelizable
        """ A string which is passed to `GNU Parallel` when the pipeline is computed using the generated Bash Script. """

        self.touched:bool = False
        """ Overrides determinig up-to-dateness; if `True`, forces this target to be considered out-of-date. """

        self.depth:int = 0
        """ How many levels of prerequisities does this target have. Used for generating Bash script with GNU Parallel. """
        for d in depends.values():
            self.depth:int = max(self.depth, d.depth + 1)

        self.maker:Callable = maker
        """
        A callable which takes the parent targets and processes them into this target.

        The `self.maker` should not care whether the computation is performed in-memory or file-wise. It just uses the load() and save() commands.
        """

        self.value:Union[None,Any] = None
        """ A working computed value associated with this object.

        It gets assigned in two possible ways:
        1. Lazily, when call to `self.get()` is made and `self.value` is still `None`, then `self.load()` fetches the data from `self.path`.
        2. When some runtime value is provided with `self.set()`. This typically happens during making of this target or during runtime operating, surrogating original file-based data.
        """

    @abstractmethod
    def load(self) -> None:
        """ This has to recover value stored in `self.path` to `self.value`. """
        pass

    @abstractmethod
    def save(self) -> None:
        """ Takes the data in `self.value` and stores it into `self.path`. """
        pass

    def sanity_checker(self, value) -> None:
        """ Override this to implement type checks or other goodies. """
        pass

    @final
    def set(self, value, auto_save_override:Union[bool,None]=None) -> None:
        """ Given a value, sets `self.value` with possible data integrity or type checks. Does not care where the data came from. """
        self.sanity_checker(value)
        save = auto_save_override if auto_save_override is not None else GenericDataType.AUTO_SAVING_TO_FILE
        if save:
            print(f"Auto saving {self} to FILE!", file=sys.stderr)
            self.save()

    @final
    def get(self) -> Any:
        """ Implements lazy loading. """
        if self.value is None:
            print(f"Loading target {self} from FILE!", file=sys.stderr)
            self.load()
            assert self.value is not None, "The value was apparently not loaded in fact."
        return self.value

    @final
    def make(self, recurse=True, force=False):
        """ A generic tool which backtracks all source targets and makes them if not ready.

        `recurse`: Check up-to-dateness recursively. If False, this target takes its prequisities even tough they may not be up-to-date.
        `force` : All dependencies are made regardless of their up-to-dateness.
        """

        # Measure time.
        print(f"{self}: Start making with {len(self.depends)} dependencies.", file=sys.stderr)
        time_start = time.time()

        # Make the prequisities. The recursivity must not be wanted, e.g., when manually experimenting / debugging a signlge pipeline stage.
        if recurse:
            for _, dep in self.depends.items():
                dep.make(recurse, force)

        # The criterion of whether to launch the associeated maker function.
        if force or not self.is_up_to_date():
            print(f"{self}: Launch maker {self.maker}", file=sys.stderr)
            time_make_start = time.time()
            self.maker(self)
            time_make_duration = time.time() - time_make_start
            time_make_sec = time_make_duration // 60
            time_make_min = int(time_make_duration / 60)
            print(f"{self}: Maker function done in {time_make_min}min {time_make_sec:.2}s.", file=sys.stderr)
        else:
            print(f"{self}: Already made.", file=sys.stderr)

        # Timing.
        duration = time.time() - time_start
        time_sec = duration // 60
        time_min = int(duration / 60)
        print(f"{self}: Made in {time_min}min {time_sec:.2}s.", file=sys.stderr)

    @abstractmethod
    def str_detailed(self):
        """ Some sort of detailed description. """

    @final
    def mark_as_touched(self):
        """ ... """
        #print(f"Marking {self} as touched!")
        self.touched = True

    @final
    def __repr__(self) -> str:
        color = fg("green") if self.is_up_to_date() else fg("red")
        reset = attr("reset")
        return f"{color}{str(self.path)}{reset}{attr('dim')} [{self.depth}@{self.parallelizable}jobs] ({type(self).__name__}){reset}"# + ("\n" + x if len(x.strip()) > 0 else "")

    @final
    def mtime(self) -> float:
        """ Returns the modification time. For directories, the oldest modification time among all descendatns is returned. """
        # If the file is not ready yet...
        if not self.path.exists():
            return float("-inf")
        else:
            ret = os.path.getmtime(self.path.resolve())
            if self.path.is_dir():
                for g in self.path.rglob("*"):
                    ret = min(os.path.getmtime(g.resolve()), ret)
            return ret

    @final
    def is_up_to_date(self) -> bool:
        """ Up-to-dateness criterion. """
        if self.touched:
            return False
        for preq in self.depends.values():
            assert isinstance(preq, GenericDataType)
            if not preq.is_up_to_date():
                return False # If any of prequisities is out-of-date, I am as well.
            if self.mtime() < preq.mtime():
                return False # If any of prequisite is newer than this target, I am out of date.
        return self.path.exists() # If all prequisities are OK, I might not be.
