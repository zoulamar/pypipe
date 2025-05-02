# pypipe

Data evaluation and experimentation framework.
Developed during the course of my [master thesis](http://hdl.handle.net/10467/107070) to keep track of different machine learning experiments and settings I had to try out and compare.
The pypipe can be likened to a python-based build system with several strong conventions to keep the experimentation sanity.
An experiment is a collection of computational stages, each taking some inputs and producing some outputs.
Commonly, one source data is processed by several different versions of an algorithm, creating nested tree-like fork structure of the modified data.
The hierarchy of stages is strictly matched to filesystem tree hierarchy; Python stage modules are found locally or among standard modules.
Any experiment stems from a root stage, which is identified as a Python module derived from the `RoodModule` class.
Each stage comprises files (targets) either direct (static, already existent), or indirect (to be created from results in the previous stage or by e.g. downloading from the internet).
There can be multiple data instances which can be evaluated in parallel, each such instance is a separate file / target; the pypipe is can generate a GNU Parallel script to run all the stages with full CPU utilization.
For details, please read the documentation in the code; proper introductions and manuals to be done.

# Installation

Unfortunately, as of 2025, current installation via setuptools is broken - dependencies are not installed automatically.
Will be fixed when needed.

# Usage:


# In comparison to ...

Of course, there is plenty of other, likely more performant or simply anyhow "better", approaches to the same problem.
(Everyone has to organise their data somehow.)
Although this package does not aspire to become world-class package, still some aspects might be of interest to other people around; that is why I published it at all.
The tools having some basal technicalities (data integrity, intermediate result caching, ...) in common, the final decision to use one or another might be a matter of personal tase after all.
Below are some remarks on different tools I know about.

## [Data Version Control (DVC)](https://dvc.org/).

Very nice and advanced tool, `pypipe` being very similar to DVC in many aspects. However,

- DVC requires defining pipeline structrure "apart" from source codes and data. `dvc.yaml` file is being magically parsed from yaml, the pipeline is defined too rigidly in this syntax. On the contrary, `pypipe` relies on directory mapping, which inherently yields tree (or DAG with symlinks) structure of the experimentation. Pypipe loses nothing.
- DVC enforces certain semantics on certain files ([metrics](https://dvc.org/doc/user-guide/project-structure/dvcyaml-files#metrics), params, artifacts). `pypipe` on the contrary is agnostic about the semantics and leaves it up to the user. (User needs to write collector or plotting scripts or use some in the standard library.)
- `pypipe` is cleaner and more succinct. Does not repeat itself such as in `dvc.lock` file. User versions what needs to be versioned, relying on repeatability of each computational stage. Of course, if some higher-level sanity on computation consistency is required, a checker stage can be provided.
- `pypipe` standardizes data interfaces and data-structure integrity checks, which could make it reusable.

## [Dr. Watson](https://juliadynamics.github.io/DrWatson.jl/stable/)

At least according to [rationale](https://juliadynamics.github.io/DrWatson.jl/stable/#Rationale), DrWatson aims to resolve the very same issue as `pypipe`. It looks very nice. However,

- Integrating git hashes into filenames is wrong. The pipeline has to be consistent and the data, if they are derived, has to be replicated to the very same state given one has the same
- Library and environment dependencies are definitely necessary, but why mixing it with a build system?! Let's aim for doing one thing but doing it right. (Not claiming `pipipe` does it right... I imagine so many points of complaints... Yet at least do not attempt to solve something which is satisfactorily solved already - i.e., a project using `pypipe` is supposed to select a virtual env manager such as rye or venv and the user has to set it up or use a specific rooty pypipe stage, which does it automatically.)
- Tutorial requires you to use some interactive shell. If you really mean it, you will have to deal with files and internal structure of the tool after all. `pypipe` lets you think in the terms of files and stages from the start - because there is really nothing more behind it.
- Dr. Watson is bound to Julia. Similarly `pypipe` is bound to Python. Subprocesses can execute anything runnable, but they lose some sanity checking of the given tool.

## [Weights and Biases](https://docs.wandb.ai/)

- Paywall, account, vendor lock, ...
- Artifacts and runs instead of files and "stages" in `pypipe`.
- Seems like it is heavily cloud-oriented. Again, why not, if someone needs to store artifacts on server, let them have it; great tools exist to do the job.

## Doing it by hand / custom scripts

Surprisingly a lot of researchers manage their data by hand or curate their own magic scripts to keep the sanity.
Practically they emulate all the functions of abovementioned tools with the risk of making and inadvertent mistake...

