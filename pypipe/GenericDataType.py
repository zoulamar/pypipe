#!/usr/bin/echo Script not meant as executable:

import sys
from abc import ABC, abstractmethod
import os, time
from pathlib import Path
from typing import Callable, Dict, Iterator, Tuple, Type, Union, Any, final
from colored import fg, attr

class GenericDataType(ABC):
    """ Class which facilitates these things:
        - Consistent filesystem unit abstraction with strict Python semantics. (Python data-oriented object with its 1-to-1 filesystem counterpart. Plus extra sweet stuff like methods etc.)
        - Dependencies between targets. (Recursive up-to-dateness evaluation, minimal update detection, ...)
        - A method reference to factually make this specified target. Such method needs to have correct input targets defined.
        - Standard saving and loading wrapper.
        - File-independent data exchange format. I.e., allows passing data in-memory.

    I.e., each datatype lists its dependent values plus keeps a reference to a correct method which can create this target given the dict of dependencies.

    Every instance of GenericDataType has to have a filesystem couterpart.
    Also, every instance of GenericDataType has to have a list of source files in order to determine whether this file is up-to-date.
    """

    USED_PATH_REGISTRY:Dict[Path,"GenericDataType"] = {}
    """ This is to ensure that one file does not accidentaly have two different coexisting Target objects or two instances of the same target loaded in memory. """

    PRODUCTION_MODE:bool = False
    """ This global switch turns off certain target recomputation.

    Typically, it is not desirable to "learn" target when predicting signals.

    TODO: Replace with some sort of context manager / environment variable / ... I.e., such that
    ```
    with GenericDataTypeFactory.alive() as f:
        f.new(...)
    ```
    """

    @staticmethod
    def filteritems(d:dict,t:Type)->Iterator[Tuple[str,"GenericDataType"]]:
        """ Generates tuple `name, value` only if `value` has same type as self. """
        for n, v in d.items():
            if isinstance(v, t):
                yield n,v

    @staticmethod
    def nop(*_, **__):
        """ A placeholder callable `maker` used when the target is not made via this system. """
        return True

    def __init__(self, where:Path, maker:Union[None,Callable], depends:Dict[str,"GenericDataType"] = {}, params:Any=None, parallelizable:str="100%", ignore_on_production_mode:bool=False):
        """ The `GenericDataType` has to be instantiated with some particular filesystem path which may be used for persistent storage of this target's value.

        However, the target can be also passed some runtime data via `self.set(...)`, bypassing those stored on `where`.
        This is useful when the Pipeline is already trained and used only for computing the predictions.
        """

        # Do not allow duplicate instances referring to the same filesystem path!
        if isinstance(self, SourceCodeDataType):
            if where in GenericDataType.USED_PATH_REGISTRY:
                self = GenericDataType.USED_PATH_REGISTRY[where]
                return
        else:
            assert where not in GenericDataType.USED_PATH_REGISTRY, f"Module at path {where} already present:\n{pformat(GenericDataType.USED_PATH_REGISTRY)}"
            GenericDataType.USED_PATH_REGISTRY[where] = self

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

        self.depth = 0
        """ How many levels of prerequisities does this target have. Used for generating Bash script with GNU Parallel. """
        for d in depends.values():
            self.depth = max(self.depth, d.depth + 1)

        self.maker:Callable = GenericDataType.nop
        """
        A callable which takes the parent targets and processes them into this target.

        The `self.maker` does not care whether the computation is performed in-memory or file-wise. It just uses the load() and save() commands.
        """
        if maker is not None:
            self.maker = maker

        self.value:Union[None,Any] = None
        """ A working computed value associated with this object.

        It gets assigned in two possible ways:
        1. Lazily, when call to `self.get()` is made and `self.value` is still `None`, then `self.load()` fetches the data from `self.path`.
        2. When some runtime value is provided with `self.set()`. This typically happens during making of this target or during runtime operating, surrogating original file-based data.
        """

        self.ignore_on_production_mode:bool = ignore_on_production_mode
        """ If `True` and `GenericDataType.PRODUCTION_MODE == True` then `self.make()` will always believe this Target is up-to-date.

        Useful for "learning" targets. """

    def transformer(self, value):
        """ This method is used by `set()` to transform miscelenaus data format to consistent internal repr.

        E.g., useful for converting ROS messages.
        """
        return value

    @abstractmethod
    def load(self):
        """ This has to recover value stored in `self.path` and pass it to `self.set()` which is a method which checks data integrity etc... """
        pass

    @final
    def set(self, value):
        """ Given a value, sets `self.value` with possible data integrity or type checks. Does not care where the data came from. """
        self.value = self.transformer(value)
        if self.PRODUCTION_MODE:
            print(f"Production mode. Not saving. The value of {self} is set.") #  to {self.value}.
            self.mark_as_touched()
        else:
            print("Std. mode. Saving.")
            self.save()

    @abstractmethod
    def save(self):
        """ Takes the data in `self.value` and stores it into `self.path`. """
        pass

    @final
    def get(self):
        """ Implements lazy loading. """
        if self.value is None:
            print(f"Loading target {self} from FILE!", file=sys.stderr)
            self.load()
            assert self.value is not None
        return self.value

    @final
    def make(self, recurse=True, force=False):
        """ A generic tool which backtracks all source targets and makes them if not ready.

        `recurse`: Check up-to-dateness recursively. If False, this target takes its prequisities even tough they may not be up-to-date.
        `force` : All dependencies are made regardless of their up-to-dateness.
        """

        if self.ignore_on_production_mode and GenericDataType.PRODUCTION_MODE:
            print(f"{self}: Proclaimed as Done.", file=sys.stderr)
            return

        # Measure time.
        print(f"{self}: Start making with {len(self.depends)} dependencies.", file=sys.stderr)
        time_start = time.time()

        # The recursivity must not be wanted, e.g., when manually experimenting / debugging a signlge pipeline stage.
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
        """ Some sort of detailed output. """

    @final
    def mark_as_touched(self):
        #print(f"Marking {self} as touched!")
        self.touched = True

    def __repr__(self) -> str:
        color = fg("green") if self.is_up_to_date() else fg("red")
        reset = attr("reset")
        return f"{color}{str(self.path)}{reset}{attr('dim')} [{self.depth}@{self.parallelizable}jobs] ({type(self).__name__}){reset}"# + ("\n" + x if len(x.strip()) > 0 else "")

    def mtime(self) -> float:
        """ Returns the modification time. For directories, the oldest modification time among all descendatns is returned. """
        if not self.path.exists():
            return float("-inf")
        ret = os.path.getmtime(self.path.resolve())
        if self.path.is_dir():
            for g in self.path.rglob("*"):
                ret = min(os.path.getmtime(g.resolve()), ret)
        return ret

    def is_up_to_date(self) -> bool:
        """ Up-to-dateness criterion.

        Uses filesystem modification time. However, perhaps some different criterion will be used later.
        """
        #print(f"Is target {self.path} up to date?")
        if self.ignore_on_production_mode and GenericDataType.PRODUCTION_MODE:
            #print(f"Target {self.path} is ignored on production mode.")
            return True
        if self.touched:
            #print(f"Target {self.path} is touched.")
            return False
        for preq in self.depends.values():
            #print(f"Inspect prequisite {preq.path}")
            if isinstance(preq, GenericDataType):
                #print(f" ... is GenericDataType")
                if preq.mtime() > self.mtime() or not preq.is_up_to_date():
                    #print(f" ... mtime {preq.mtime()}, self mtime {self.mtime()}, {preq.is_up_to_date()}")
                    return False
            else:
                #print(f"Inspect gettime {preq.path}")
                if os.path.getmtime(preq) > self.mtime():
                    #print(f"  ... out of date")
                    return False
        #print(f"Target {self.path} is up-to-date.")
        return True
