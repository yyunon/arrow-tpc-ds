from ctypes import *
from subprocess import *
import pathlib


def dbgen():
    tpchkit = ImportModules("tpch")
    dbgen_functor = tpchkit.dataset_generator
    print(dbgen_functor.main(2, "-h"))


class ImportModules:
    def __init__(self, module):
        print("This module is still WIP...")
        self.so = None
        if module == "tpch":
            self.so = "/usr/local/lib/dbgen.so"
        else:
            print("TODO: Make this intelligent")
            print("Error: You did not install tpch-kit, exiting...")
            assert(False)

        self.dataset_generator = CDLL(self.so)
