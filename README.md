# dagger
Declaratively convert data engineering functions to DAGs

## Motivation
Feature engineering can often be hampered by cluttered and arcane transformation code.
Transformation functions, where they exist, often combine multiple feature definitions
grouped semantically. At the same time, it is difficult to immediately trace out the lineage
of any single variable: are there further dependencies? What if a script spans multiple
files? Further, the order of execution of these functions are likely very specific
and it is difficult to immediately ascertain the implications of changing that order.

All of these complications make maintaining transformation scripts a pain. Orchestration
tools do not solve this problem out-of-the-box, as nodes or tasks must still be defined.
`dagger` is a simple, lightweight package for managing complex transformation code.
Simply declare features as functions, which will be parsed and converted to a repeatable
execution plan that can then be applied to your data.

For instance, consider this transformation function:
```
def my_transform(data: pd.DataFrame) -> pd.DataFrame:
    assert {'a', 'b'} in set(data)

    data = data.copy()

    data['ab'] = data['a'] + data['b']
    data['a2'] = data['a'] ** 2
    return data
```

Not only is there boilerplate, it's not immediately clear how the inputs and outputs
interaction with other transforms. Instead, the transformation can be represented as:
```
def ab(a, b):
    return a + b


def a2(a):
    return a ** 2
```

This makes each feature atomic, lightweight, and modular! `dagger` aims to reduce
time managing feature transforms, enabling the researcher to spend more time acquiring
and applying domain expertise.

## Installation
```
git clone https://github.com/czou-1107/dagger.git
pip install ./dagger
```

## Usage
To use in a Python environment:

```
from dagger.dag import DagExecutor

script_path = ...  # Path to script(s)
data = ...  # pandas DataFrame or dict


executor = DagExecutor()
executor.add_function_scripts(script_path)
executor.plan()
executor.visualize()  # Plot inferred dependency graph
executor.apply(data)
```

Alternately, a CLI can be invoked from terminal:

```
dagger --scripts $script_path --data $data_path
```

Sample scripts can be found in `samples/`
