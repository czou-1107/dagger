# This script is to be read in as text and processed!
# It is paired with a data fixture: data.py


def var0(var1, var2):
    return max(var1, var2)


def var1(a: float, b: float):
    return a + b


def var2(var1, c: float):
    return var1 * c


def var3(var2, a: float):
    return var2 - a


THIS_IS_NOT_A_FUNCTION = 1


class ThisIsNotAFunction:
    pass
