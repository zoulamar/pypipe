#!/usr/bin/echo Script not meant as executable:

"""
Filename:  __init__.py
Author:    Ing. Martin Zoula (zoulamar@fel.cvut.cz)

The core library functionality to provide dataset processing pipelining. Note, that with a single run of the librarym, the PIPELINE_ROOT_PATH, USER_SOURCE_SPACE and VERBOSE module-level variables need to be set properly.
(TODO? Load it from ENV?)
"""

import sys
from abc import ABC, abstractmethod
import os, time
from pathlib import Path
from typing import Callable, Dict, Iterator, Literal, Tuple, Type, Union, Any, final
from colored import fg, attr
from pprint import pformat
from abc import ABC, abstractmethod
import importlib, os, sys
from pathlib import Path
from pprint import pformat
from typing import Dict, List, Union, final, Type, Iterator, Tuple

USER_SOURCE_SPACE:Path|None = None
""" Directory with user-provided computational modules. """
VERBOSE:bool = False
""" Enable verbose debug output to see what is pypipe doing. """

class BaseModule(ABC):
    """ This class provides a convention of a meaningful organizing of the targets.

    This class wraps together a semantically related targets.
    You can think about it as a namespace with an unique set of targets and computation methods.
    The module dwells at a unique directory where it stores its computational by-products and configurations.
    A Module provides a set of named computational targets (instances of `GenericDataType`) which then form a target dependency tree.
    By convention, the targets dwell in the module's directory to facilitate unambiguous namespacing.

    Note that the "targets" realize the build system.
    This modular system serves as a tool for meaningful management of the targets.

    By convention, there are two types of targets, the primary targets and other targets.
    The primary targets do not have colon (:) in their name.
    Other targets use the colon as long as they are related to the primary target.
    """

    ACTIVE_MODULE_REGISTRY:Dict[Path,"BaseModule"] = {}
    """ A lookup table (Path->BaseModule instance) to prevent loading of duplicate Modules. """

    @final
    @staticmethod
    def module_lazy_loader(path:Path) -> "BaseModule":
        """ Match a Python module with given sub-directory of a selected pipeline root. Get a module at given path efficiently without duplicate instances. """

        # Misc. start.
        assert path.is_dir(), f"The pypipe system requires modules to be directories. Given path {path} is not."
        pymodule_name:str = path.stem # NOTE: This separates the stage's label from stage module.
        sep = f"{path.name}'s "
        if VERBOSE:
            print(f"{sep}module_lazy_loader: Module path: {path}")
            print(f"{sep}module_lazy_loader: Module name: {pymodule_name}")

        # Perform caching. For a given path, the module is unique.
        if path in BaseModule.ACTIVE_MODULE_REGISTRY:
            if VERBOSE: print(f"{sep}module_lazy_loader: Using cached module.")
            return BaseModule.ACTIVE_MODULE_REGISTRY[path]

        # Here the lookup for sources will happen.
        pysrc:Path|None = None
        cls:type|None = None
        for ending in (pymodule_name + '.py', 'source.py', 'src.py'):
            if VERBOSE: print(f"{sep}module_lazy_loader: Probe {path/ending}")
            if (path / ending).is_file():
                pysrc = path / ending
                if VERBOSE: print(f"{sep}module_lazy_loader: Found a source file in module's directory {pysrc}")
                break
        else:
            # If no ad-hoc modules in the directory itself, try project's source space.
            if USER_SOURCE_SPACE is not None:
                results = sorted(USER_SOURCE_SPACE.glob(f"**/{pymodule_name}.py"))
                if VERBOSE: print(f"{sep}module_lazy_loader: Probe source space {USER_SOURCE_SPACE} -> {pformat(results)}")
                if len(results) > 0:
                    selected_result = results[0]
                    if VERBOSE: print(f"{sep}module_lazy_loader: Found a source file in source space {selected_result}")
                    pysrc = selected_result.resolve()

            # If still nothing found, fallback to std. modulees.
            if pysrc is None:
                if VERBOSE: print(f"{sep}module_lazy_loader: Probe std_modules.")
                try:
                    import pypipe.modules
                    cls = pypipe.modules.__dict__[pymodule_name]
                    assert cls is not None
                    pysrc = Path("<std_modules>")
                    if VERBOSE: print(f"{sep}module_lazy_loader: Candidate: {cls}")
                    assert issubclass(cls, BaseModule)
                    if VERBOSE: print(f"{sep}module_lazy_loader: Found std module {cls}")
                except Exception as e:
                    e.add_note("")
                    raise

        if pysrc is None and cls is None:
            raise ModuleNotFoundError(f"module_lazy_loader: Module for path {path} could not be resolved.")
        elif cls is None and pysrc is not None:
            if VERBOSE: print(f"{sep}module_lazy_loader: Resulting python module to be loaded {pysrc}")

            pysrc = pysrc.relative_to(Path(os.getcwd()))
            if VERBOSE: print(f"{sep}module_lazy_loader: Relative path {pysrc}")

            pymodspec = str(pysrc.parent / pysrc.stem).replace("/", ".")
            if VERBOSE: print(f"{sep}module_lazy_loader: Pymodspec {pymodspec}")

            pysrc = pysrc.parent
            while True:
                path_to_add = str(pysrc.resolve())
                if not path_to_add in sys.path:
                    if VERBOSE: print(f"{sep}module_lazy_loader: Adding {path_to_add} to PATH.")
                    sys.path.append(path_to_add)
                elif VERBOSE: print(f"{sep}module_lazy_loader: {path_to_add} already in PATH.")
                if pysrc.resolve() == Path.cwd():
                    break
                pysrc = pysrc.parent

            try:
                pymod = importlib.import_module(pymodspec)
            except ModuleNotFoundError:
                print("The module could not be imported. Either the passed directory is not a Module or the Module source is not in Python path.")
                raise
            if VERBOSE: print(f"{sep}module_lazy_loader: Imported module {pymod}")

            candidate_names = (pymodule_name, "Module", "Main")
            for candidate_name in candidate_names:
                if hasattr(pymod, candidate_name):
                    cls = getattr(pymod, candidate_name)
                    if callable(cls) and issubclass(cls, BaseModule):
                        if VERBOSE: print(f"{sep}module_lazy_loader: Module type {cls}")
                        break
            else:
                raise AttributeError(f"None of candidate_names {candidate_names} was found in the module {pymodule_name}")
            if VERBOSE: print(f"{sep}module_lazy_loader: Final class in the module: {cls}")
        else:
            assert cls is not None

        # Load it, finally.
        instance = cls(path)
        if VERBOSE: print(f"{sep}module_lazy_loader: Fresh instance {instance}")
        BaseModule.ACTIVE_MODULE_REGISTRY[path] = instance
        return instance

    def __init__(self, module_path:Path, is_root_module:bool = False) -> None:
        """ The task now: Prepare the module for a computation. Instantiate Parent Module. Register all output files, either currently available or to-be-computed-yet. Perform sanity checks. """
        super().__init__()

        # Is the module valid, at least basically? Aint it duplicate?
        assert module_path.is_dir()
        assert module_path.name.split(".")[0] if "." in module_path.name else module_path.name == self.__class__.__name__
        assert module_path not in BaseModule.ACTIVE_MODULE_REGISTRY, "Trying to instantiate a duplicate Module. Use BaseModule.ACTIVE_MODULE_REGISTRY[path_to_module] instead of creating a new one."
        self.module_path = module_path
        """ A directory containing this module's targets. """

        self.parent_module:BaseModule|None = None if is_root_module else BaseModule.module_lazy_loader(self.module_path.parent)
        """ Previous computational stage. None iff this is the root node.  """

        self.targets:Dict[str,GenericDataType] = self.declare_targets()
        """ All declared targets. Anyone can reference to them, use them as dependencies or make them by calling their make method. """

        # Inject dependency on source code of the module itself. NOTE: Not sure if necessary. TODO: Or configure it by local pypipe.yaml
        source_code_target = GenericDataType(where=self.module_path, maker=GenericDataType.expect_made)
        for _, v in self.targets.items():
            v.depends["__module_source_code__"] = source_code_target

        self.make_gitignore()

    def make_gitignore(self) -> None:
        """ Updates a .gitignore file with all targets. """
        with open(self.module_path / ".gitignore", "w") as f:
            for t in self.targets.values():
                f.write(str(t.path.relative_to(self.module_path)) + "\n")
            for t in self.extra_gitignore():
                f.write(t + "\n")

    def extra_gitignore(self) -> List[str]:
        """ Override this to declare extra patterns to be ignored by Git. """
        return []

    @abstractmethod
    def declare_targets(self)->Dict[str,"GenericDataType"]:
        """ Every module must be able to declare what outputs it provides.

        This is a Module-specific behaviour, it may depend on current configuration or whatever else.
        Every target is a GenericDataType instance with correctly set dependencies and `make` method.
        """

    def __repr__(self) -> str:
        return f"Module {type(self)} at {self.module_path}"# with config: \n{self.config}"

    def get_parent(self) -> "BaseModule":
        if self.parent_module is None:
            raise ValueError
        return self.parent_module

    def get_root_module(self) -> "BaseModule":
        inspected = self
        while inspected.parent_module is not None:
            inspected = inspected.parent_module
        return inspected

    def find_ancestor_module(self, what:str) -> Union["BaseModule",None]:
        """ Returns parent module of given name or label. """
        if self.module_path.name == what or self.module_path.suffix == what:
            return self
        if self.parent_module is None:
            return None
        return self.parent_module.find_ancestor_module(what)

    @final
    def enumerate_pipeline(self) -> List["BaseModule"]:
        mod_list:List[BaseModule] = [self]
        while mod_list[-1].parent_module is not None:
            mod_list.append(mod_list[-1].parent_module)
        return reversed(mod_list) # type: ignore

    @final
    def codename_pipeline(self):
        return "-".join(map(lambda x : x.codename(), self.enumerate_pipeline()))

    def codename(self):
        return self.module_path.name

    @final
    def targets_by_type(self, t:Type)->Iterator[Tuple[str, "GenericDataType"]]:
        for n, v in self.targets.items():
            if isinstance(v, t):
                yield n,v

    def targets_primary_names(self)->Iterator[str]:
        primary_names = set()
        for name in self.targets.keys():
            name_striped:str = name.split(":")[0]
            if name_striped not in primary_names:
                primary_names.add(name_striped)
                yield name_striped

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

    @staticmethod
    def expect_made(*_, **__) -> Literal[True]:
        """ A placeholder callable `maker` used when the target is not made via this system. """
        return True

    def __init__(self, where:Path, maker:Callable, depends:Dict[str,"GenericDataType"] = {}, params:Any=None, parallelizable:str="100%") -> None:
        """ The `GenericDataType` has to be instantiated with some particular filesystem path which may be used for persistent storage of this target's value.

        However, the target can be also passed some runtime data via `self.set(...)`, bypassing those stored on `where`.
        This might be useful when the Pipeline is already trained and used only for computing the predictions.
        """

        assert where.resolve() not in GenericDataType.USED_PATH_REGISTRY, f"Target at path {where} already open:\n{pformat(GenericDataType.USED_PATH_REGISTRY)}"
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

    def load(self) -> None:
        """ This has to recover value stored in `self.path` to `self.value`. """
        raise NotImplementedError

    def save(self) -> None:
        """ Takes the data in `self.value` and stores it into `self.path`. """
        raise NotImplementedError

    def sanity_checker(self, _) -> None:
        """ Override this to implement type checks or other goodies. """
        pass

    @final
    def set(self, value, auto_save_override:Union[bool,None]=None) -> None:
        """ Given a value, sets `self.value` with possible data integrity or type checks. Does not care where the data came from. """
        self.sanity_checker(value)
        self.value = value
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
            #assert self.value is not None, "The value was apparently not loaded in fact."
        return self.value

    @final
    def make(self, recurse=True, force=False) -> None:
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

    def str_detailed(self) -> str:
        """ Some sort of detailed description. """
        return f"A generic data type at {self.path}"

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

