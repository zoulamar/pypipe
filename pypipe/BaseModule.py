#!/usr/bin/echo Script not meant as executable:

from abc import ABC, abstractmethod
import importlib
from pathlib import Path
from pprint import pformat
from typing import Dict, List, Union, final, Type
from . import GenericDataType

class BaseModule(ABC):
    """
    A prescription of a computational Module adhering to my extraordinarily overthought Convention.

    A Module dynamically provides a set of named computational targets which together form a dependency tree.
    This dependency tree may be then computed on-demand or a bash parallel job list can be exported.
    Conceptually, any Module has its directory counterpart; it loads itself accoding to the folder it is represented by, allowing the user to perform experiments, computations or debugging in some sort of sane and understandable fashion.

    In file-oriented fashion, an external tool is used to invoke a part of pipeline, e.g., for tuning of properties or debugging.
    In integrated fashion, a sequence of modules is loaded in memory together with their intermediate data.

    A Module has to be Stateless.
    A Module is in fact only a collection of executable code, the State comes always from other Targets. We simply depend on fast-enough filesystem.
    I.e., both configuration file (typically `config.yaml`) and intermediate parameter file (model parameters from learning) are just a targets.
    """

    ACTIVE_MODULE_REGISTRY:Dict[Path,"BaseModule"] = {}
    """ A lookup table (Path->BaseModule instance) to prevent loading of duplicate Modules. """

    @final
    @staticmethod
    def resolve_module_by_spec(spec:str, root:Path) -> "BaseModule":
        """ Translates some module specificator pattern into absolute module path.

        `spec`: Specification of which particular model to use. If starts with "/", then correct full path from `root` expected. If no slashes present, find by name or label.
        `root`: Pipeline root.
        """

        # E.g., the root is pwd and I want to load it.
        if Path(spec) == root:
            return BaseModule.resolve_module_by_path(root)

        # First try whether the specification matches some existing directory. If so, no other hard work needs to be done.
        if (root / spec).is_dir():
            return BaseModule.resolve_module_by_path(root / spec)

        raise NotImplementedError("The following logic was working, but I deem it messy. Hopefully I won't need to see it again soon.")

        # Find a correct folder.
        print(f"Resolving model given a specification >>{spec}<< w.r.t. selected root module: {root}")
        if len(spec.parts) == 1:
            # Search either by module type or label.
            dotcount = spec.name.count(".")
            if dotcount == 0:
                stem, ext = spec.name, ""
            elif dotcount == 1:
                stem, ext = spec.name.split(".")
            else:
                raise ValueError("Module cannot have more than one dot in name. Note, that dot separates python module name and its label.")
            print(f"Finding a module type >>{stem}<< with label >>{ext}<<.")
            if stem == "" and ext != "": # Find by label.
                hits = list(root.glob(f"**/*.{ext}/"))
            elif stem != "" and ext == "": # Find by module.label.
                hits = list(root.glob(f"**/{stem}*/"))
            elif stem != "" and ext != "": # Find by module.label.
                hits = list(root.glob(f"**/{stem}.{ext}/"))
            else:
                raise ValueError("Not supported.")
            assert len(hits) == 1, f"Modules have to be labeled uniquely in order to find them thus. Candidates: \n{pformat(hits)}"
            module_abs = hits[0]
        else:
            # Explicit determination.
            if spec.parts[0] in ("/") or spec.parts[0].startswith("PipelineRoot"):
                spec = Path(*spec.parts[1:])
            module_abs = root / spec
            assert module_abs.is_dir()
        print(f"Resolved module path: {module_abs.resolve()}")
        return BaseModule.resolve_module_by_path(module_abs)

    @final
    @staticmethod
    def resolve_module_by_path(path:Path) -> "BaseModule":
        """ Quite simply inspects a Module's folder name and selects a correct Python module to use. """
        if path in BaseModule.ACTIVE_MODULE_REGISTRY:
            return BaseModule.ACTIVE_MODULE_REGISTRY[path]
        pymodule_name = path.stem
        loaded = BaseModule.module_lazy_loader(pymodule_name)(path)
        return loaded

    @final
    @staticmethod
    def module_lazy_loader(pymodule_name) -> Type["BaseModule"]:
        """ This is a function which returns a callable `class` efficiently.

        This implementation relies on hardcoded list of allowed computational modules in order to make the module loading lazy.
        I.e., the Python's `importlib` is invoked only once the particular module is in fact required.
        I believe it can be made implicit via some sort of module discovery, but for now it is not deemed necessary.
        TODO: Make some efficient module discovery mechanism.

        Note: Below is original implicit implementation, but that meant that all modules had to be loaded regardless of what was used in practice.
        ```
        from Filter import Filter
        from Select import Select
        ...
        mod = globals()
        MODULES_here = {n : mod[n] for n in mod.keys() if isinstance(mod[n], type) and issubclass(mod[n], BaseModule) and mod[n] != BaseModule}
        ```
        """

        # This is a list of all registered (and thus allowd) Pipeline Modules. If not
        MODULE_LOOKUP = {
            "PipelineRootCommsRosified" : __name__,
            "Filter" : "Filter",
            "Augment" : "Augment",
            "Forge" : "Forge",
            "Umap" : "Umap",
            "Poly" : "Poly",
            "Report" : "Report",
            "Bedford" : "Bedford",
            "PolyForWindowing" : "PolyForWindowing",
            "FSPL" : "FSPL",
            "NN" : "NN",
            "GP" : "GP",
            "Interpolator" : "Interpolator",
            "StaticError" : "StaticError",
        }
        #print(f"module_lazy_loader: {MODULE_LOOKUP}")
        pymod = importlib.import_module(MODULE_LOOKUP[pymodule_name])
        #print(f"module_lazy_loader: {pymod}")
        cls = getattr(pymod, pymodule_name)
        #print(f"module_lazy_loader: {cls}")
        assert callable(cls), f"The class {cls} from module {pymod} is not callable. Is it not still abstract?"
        assert issubclass(cls, BaseModule)
        assert cls != BaseModule
        return cls

    def __init__(self, module_path:Path) -> None:
        """ The task now: Prepare the module for a computation. Instantiate Parent Module. Register all output files, either currently available or to-be-computed-yet. Perform sanity checks. """
        if not hasattr(self, "is_root_module"):
            self.is_root_module = False
        super().__init__()

        # Is the module valid, at least basically? Aint it duplicate?
        assert module_path.is_dir()
        assert module_path.name.split(".")[0] if "." in module_path.name else module_path.name == self.__class__.__name__
        assert module_path not in BaseModule.ACTIVE_MODULE_REGISTRY, "Trying to instantiate a duplicate Module. Use BaseModule.ACTIVE_MODULE_REGISTRY[path_to_module] instead of creating a new one."
        self.module_path = module_path
        BaseModule.ACTIVE_MODULE_REGISTRY[module_path] = self

        # Always load a parent module. I.e., recursively load whole Pipeline's fork and determine a cleanness. Use a global Module Registry not to have duplicate Module folders.
        if not self.is_root_module:
            self.parent_module = BaseModule.resolve_module_by_path(self.module_path.parent)
        else:
            self.parent_module = None

        # Now find out all provided outputs in order to perhaps compute one later.
        self.targets:Dict[str,GenericDataType] = self.declare_targets()

    @abstractmethod
    def declare_targets(self)->Dict[str,"GenericDataType"]:
        """ Every module must be able to declare what outputs it provides.

        This is a Module-specific behaviour, it may depend on current configuration or whatever else.
        Every target is a GenericDataType instance with correctly set dependencies and `make` method.
        """

    def __repr__(self):
        return f"Module {type(self)} at {self.module_path}"# with config: \n{self.config}"

    def find_parent_module(self, what:str) -> Union["BaseModule",None]:
        """ Returns parent module of given name. """
        if self.module_path.name == what or self.module_path.suffix == what:
            return self
        if self.parent_module is None:
            return None
        return self.parent_module.find_parent_module(what)

    @final
    def enumerate_pipeline(self) -> List["BaseModule"]:
        mod_list = [self]
        while mod_list[-1].parent_module is not None:
            mod_list.append(mod_list[-1].parent_module)
        return reversed(mod_list) # type: ignore

    @final
    def codename_pipeline(self):
        return "-".join(map(lambda x : x.codename(), self.enumerate_pipeline()))

    def codename(self):
        return self.module_path.name

