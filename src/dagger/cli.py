"""
Command line entrypoint
"""
from typing import List, Union

import fire

from .dag import DagExecutor


def plan_apply_dag(scripts: Union[str, List[str]], data: str, output: str):
    executor = (DagExecutor()
                .add_function_scripts(scripts)
                .plan()
    )
    result = executor.apply(data)
    result.to_parquet(output)


def main():
    fire.Fire(plan_apply_dag)
