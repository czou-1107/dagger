"""
Utility to import a script and extract its transform functions
"""
import sys
from functools import reduce
from importlib import import_module
from inspect import getmembers, isfunction
from pathlib import Path
from typing import Callable, Dict, Tuple, Union


def _resolve_script_path(path: Union[str, Path], check: bool = True) -> Path:
    """ Homogenize module path to file path, e.g. mod.submod to ../mod/submod.py """
    if isinstance(path, str) and not path.endswith('.py'):
        # Assume path is given as module, i.e. mod.submod.script
        path_parts = path.split('.')
        path_parts[-1] += '.py'
        path = reduce(lambda x, y: x / y, path_parts, Path())
    path = Path(path).resolve()

    if check and not path.exists():
        raise FileNotFoundError(path)
    return path


def _split_path(path: Path) -> Tuple[str, str]:
    """ Split module path to (directory, module name) """
    return str(path.parent), path.stem


def _get_module_funcs(mod) -> Dict[str, Callable]:
    """ Get functions from module """
    return dict(member for member in getmembers(mod, isfunction))


def extract_module_functions(script: Union[str, Path]) -> Dict[str, Callable]:
    """ Import a script and extract its functions """
    script_path = _resolve_script_path(script)
    script_dir, script_mod = _split_path(script_path)
    # Hack: temporarily add script dir to path, to enable non-pkg import
    sys.path.insert(0, script_dir)

    try:
        module = import_module(script_mod)
    finally:
        sys.path.pop(0)

    return _get_module_funcs(module)
