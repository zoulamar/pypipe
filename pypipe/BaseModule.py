#!/usr/bin/echo Script not meant as executable:

from abc import ABC, abstractmethod
import importlib, os, sys
from pathlib import Path
from pprint import pformat
from typing import Dict, List, Union, final, Type, Iterator, Tuple
from . import GenericDataType

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
    def resolve_module_by_spec(spec:Path, root:Path, source_space:Path, verbose:bool=False) -> "BaseModule":
        """ Translates some module specificator pattern into absolute module path.

        `spec`: Specification of which particular model to use. If starts with "/", then correct full path from `root` expected. If no slashes present, find by name or label.
        `root`: Pipeline root.
        """

        # E.g., the root is pwd and I want to load it.
        if Path(spec) == root:
            return BaseModule.module_lazy_loader(root, source_space, verbose)

        # First try whether the specification matches some existing directory. If so, no other hard work needs to be done.
        if (root / spec).is_dir():
            return BaseModule.module_lazy_loader(root / spec, source_space, verbose)

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
    def module_lazy_loader(path:Path, source_space:Path, verbose=False) -> "BaseModule":
        """ Get a module at given path efficiently without duplicate instances. """

        assert path.is_dir(), f"The pypipe system requires modules to be directories whereas given {path} is not."

        pymodule_name:str = path.stem
        if verbose:
            print(f"module_lazy_loader: Module path: {path}")
            print(f"module_lazy_loader: Module name: {pymodule_name}")
            print(f"module_lazy_loader: Source space: {source_space}")

        # Perform caching.
        if path in BaseModule.ACTIVE_MODULE_REGISTRY:
            if verbose: print(f"module_lazy_loader: Using cached module.")
            return BaseModule.ACTIVE_MODULE_REGISTRY[path]

        # Here the lookup for sources will happen.
        pysrc:Union[Path,None] = None
        for ending in (pymodule_name + '.py', 'source.py', 'src.py'):
            if pysrc is not None:
                break
            if verbose: print(f"module_lazy_loader: Probe {path/ending}")
            if (path / ending).is_file():
                pysrc = path / (pymodule_name+'.py')
                if verbose: print(f"module_lazy_loader: Found a source file in module's directory {pysrc}")
                if not str(path.resolve()) in sys.path:
                    sys.path.append(str(path.resolve()))
        if pysrc is None and source_space is not None:
            results = sorted(source_space.glob(f"**/{pymodule_name}.py"))
            if verbose: print(f"module_lazy_loader: Probe {pformat(results)}")
            if len(results) > 0:
                selected_result = results[0]
                if verbose: print(f"module_lazy_loader: Found a source file in source space {selected_result}")
                pysrc = selected_result
                if not str(source_space.resolve()) in sys.path:
                    sys.path.append(str(source_space.resolve()))

        assert pysrc is not None, "module_lazy_loader: Source file could not be resolved."
        if verbose: print(f"module_lazy_loader: Resulting python module to be loaded {pysrc}")

        pysrc = pysrc.relative_to(Path(os.getcwd()))
        if verbose: print(f"module_lazy_loader: Relative path {pysrc}")

        pymodspec = str(pysrc.stem).replace("/", ".")
        if verbose: print(f"module_lazy_loader: Pymodspec {pymodspec}")

        try:
            pymod = importlib.import_module(pymodspec)
        except ModuleNotFoundError:
            print("The module could not be imported. Either the passed directory is not a Module or the Module source is not in Python path.")
            raise
        if verbose: print(f"module_lazy_loader: Imported module {pymod}")

        cls = getattr(pymod, pymodule_name)
        if verbose: print(f"module_lazy_loader: Module type {cls}")

        assert callable(cls), f"The class {cls} from module {pymod} is not callable. Is it not still abstract?"
        assert issubclass(cls, BaseModule)
        instance = cls(path, source_space, verbose)
        if verbose: print(f"module_lazy_loader: Fresh instance {instance}")
        BaseModule.ACTIVE_MODULE_REGISTRY[path] = instance
        return instance

    def __init__(self, module_path:Path, source_space:Path, verbose:bool, is_root_module:bool = False) -> None:
        """ The task now: Prepare the module for a computation. Instantiate Parent Module. Register all output files, either currently available or to-be-computed-yet. Perform sanity checks. """
        super().__init__()

        # Is the module valid, at least basically? Aint it duplicate?
        assert module_path.is_dir()
        assert module_path.name.split(".")[0] if "." in module_path.name else module_path.name == self.__class__.__name__
        assert module_path not in BaseModule.ACTIVE_MODULE_REGISTRY, "Trying to instantiate a duplicate Module. Use BaseModule.ACTIVE_MODULE_REGISTRY[path_to_module] instead of creating a new one."
        self.module_path = module_path
        """ A directory containing this module's targets. """

        self.parent_module:Union[BaseModule,None] = None if is_root_module else BaseModule.module_lazy_loader(self.module_path.parent, source_space, verbose)
        """ Previous computational stage. None iff this is the root node.  """

        self.targets:Dict[str,GenericDataType] = self.declare_targets()
        """ All declared targets. Anyone can reference to them, use them as dependencies or make them by calling their make method. """

        self.verbose:bool = verbose
        """ Allows some verbose logging if desired. """

        """ Updates a .gitignore file with all targets. """
        with open(self.module_path / ".gitignore", "w") as f:
            for t in self.targets.values():
                f.write(str(t.path.relative_to(self.module_path)) + "\n")

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
        mod_list = [self]
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
