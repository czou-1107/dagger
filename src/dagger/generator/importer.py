import sys
from functools import reduce
from importlib import import_module
from inspect import getmembers, isfunction
from pathlib import Path
from typing import Tuple, Union


def _resolve_script_path(path: Union[str, Path]) -> Path:
    if isinstance(path, str) and not path.endswith('.py'):
        # Assume path is given as module, i.e. mod.submod.script
        path_parts = path.split('.')
        return reduce(lambda x, y: x / y, path_parts, Path())
    return Path(path)


def _split_path(path: Path) -> Tuple[str, str]:
    return str(path.parent.resolve()), path.stem


def _get_module_funcs(mod) -> dict:
    return dict(member for member in getmembers(mod, isfunction))


def extract_functions(script: str) -> dict:
    script_path = _resolve_script_path(script)
    script_dir, script_mod = _split_path(script_path)
    sys.path.insert(0, script_dir)

    try:
        module = import_module(script_mod)
    finally:
        sys.path.pop(0)

    return _get_module_funcs(module)
