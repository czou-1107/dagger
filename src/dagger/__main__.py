"""
Command line entrypoint
"""
from typing import List, Optional, Union

import fire

from .dag import DagExecutor


def plan_apply_dag(scripts: Union[str, List[str]],
                   data: str,
                   output: Optional[str] = None,
                   use_dask: bool = False):
    """ Plan and execute a dag from a script """
    executor = (DagExecutor(use_dask=use_dask)
                .add_function_scripts(scripts)
                .plan()
    )
    result = executor.apply(data)
    if not output:
        return result
    else:
        result.to_parquet(output)


def main():
    """ Convert to CLI """
    fire.Fire(plan_apply_dag)
