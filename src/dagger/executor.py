"""
Main package class: imports script and applies transform functions to DataFrame
"""
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Union

import pandas as pd

from .utils.importer import extract_module_functions
from .graph import ComputationGraph


@dataclass
class DagExecutor:
    """ Wrapper around ComputationGraph """
    # Is this wrapper even necessary? Potential uses:
    # - Link this to CLI, including loading and saving data
    # - Have this deal with the script loading and parsing, including multiple scripts
    scripts: List[Union[str, Path]]
    _computation_graph: ComputationGraph = field(init=False, default=None)

    def __post_init__(self):
        if isinstance(self.scripts, (str, Path)):
            self.scripts = [self.scripts]
        self._computation_graph = ComputationGraph()


    def plan(self):
        for script in self.scripts:
            funcs = extract_module_functions(script)
            self._computation_graph.add_functions(funcs)

        self._computation_graph.plan()
        return self


    def apply(self, data: pd.DataFrame) -> pd.DataFrame:
        return self._computation_graph.apply(data)


    def get_graph(self):
        return self._computation_graph._graph