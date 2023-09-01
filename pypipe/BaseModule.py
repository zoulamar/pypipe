#!/usr/bin/echo Script not meant as executable:

from abc import ABC, abstractmethod
import importlib, os, sys
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
    def resolve_module_by_spec(spec:Path, root:Path, source_space:Path) -> "BaseModule":
        """ Translates some module specificator pattern into absolute module path.

        `spec`: Specification of which particular model to use. If starts with "/", then correct full path from `root` expected. If no slashes present, find by name or label.
        `root`: Pipeline root.
        """

        # E.g., the root is pwd and I want to load it.
        if Path(spec) == root:
            return BaseModule.module_lazy_loader(root, source_space)

        # First try whether the specification matches some existing directory. If so, no other hard work needs to be done.
        if (root / spec).is_dir():
            return BaseModule.module_lazy_loader(root / spec, source_space)

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
    def module_lazy_loader(path:Path, source_space:Union[Path,None] = None) -> "BaseModule":
        """ Get a module at given path efficiently without duplicate instances. """

        assert path.is_dir(), f"The pypipe system requires modules to be directories whereas given {path} is not."

        pymodule_name:str = path.stem
        #print(f"module_lazy_loader: Module path: {path}")
        #print(f"module_lazy_loader: Module name: {pymodule_name}")
        #print(f"module_lazy_loader: Source space: {source_space}")

        # Perform caching.
        if path in BaseModule.ACTIVE_MODULE_REGISTRY:
            #print(f"module_lazy_loader: Using cached module.")
            return BaseModule.ACTIVE_MODULE_REGISTRY[path]

        # Here the lookup for sources will happen.
        pysrc:Union[Path,None] = None
        for ending in (pymodule_name + '.py', 'source.py', 'src.py'):
            if pysrc is not None:
                break
            #print(f"module_lazy_loader: Probe {path/ending}")
            if (path / ending).is_file():
                pysrc = path / (pymodule_name+'.py')
                #print(f"module_lazy_loader: Found a source file in module's directory {pysrc}")
                if not os.getcwd() in sys.path:
                    sys.path.append(os.getcwd())
        if pysrc is None and source_space is not None:
            results = sorted(source_space.glob(f"**/{pymodule_name}.py"))
            #print(f"module_lazy_loader: Probe {pformat(results)}")
            if len(results) > 0:
                selected_result = results[0]
                #print(f"module_lazy_loader: Found a source file in source space {selected_result}")
                pysrc = selected_result
                if not str(source_space.resolve()) in sys.path:
                    sys.path.append(str(source_space.resolve()))

        assert pysrc is not None, "module_lazy_loader: Source file could not be resolved."
        #print(f"module_lazy_loader: Resulting python module to be loaded {pysrc}")

        pysrc = pysrc.relative_to(Path(os.getcwd()))
        #print(f"module_lazy_loader: Relative path {pysrc}")

        pymodspec = str(pysrc.stem).replace("/", ".")
        #print(f"module_lazy_loader: Pymodspec {pymodspec}")

        pymod = importlib.import_module(pymodspec)
        #print(f"module_lazy_loader: Imported module {pymod}")

        cls = getattr(pymod, pymodule_name)
        #print(f"module_lazy_loader: Module type {cls}")

        assert callable(cls), f"The class {cls} from module {pymod} is not callable. Is it not still abstract?"
        assert issubclass(cls, BaseModule)
        instance = cls(path)
        #print(f"module_lazy_loader: Fresh instance {instance}")
        BaseModule.ACTIVE_MODULE_REGISTRY[path] = instance
        return instance

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

        # Always load a parent module. I.e., recursively load whole Pipeline's fork and determine a cleanness. Use a global Module Registry not to have duplicate Module folders.
        if not self.is_root_module:
            self.parent_module = BaseModule.module_lazy_loader(self.module_path.parent)
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

    def find_ancestor_module(self, what:str) -> Union["BaseModule",None]:
        """ Returns parent module of given name or label. """
        if self.module_path.name == what or self.module_path.suffix == what:
            return self
        if self.parent_module is None:
            return None
        return self.parent_module.find_ancestor_module(what)

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
