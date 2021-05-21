""" Type checking utilities """
from typing import Optional


def check_type(data, dtype: Optional[type] = None, action: str = 'raise'):
    if dtype is None:
        return