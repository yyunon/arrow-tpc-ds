from subprocess import *
import pathlib
import os
import sys
import shutil


def dbgen(args):
    tpchkit = ImportModules("tpch")
    mydir = os.getcwd() + "/"
    runtime_dir = tpchkit.exec_dir
    dbgen_functor = [tpchkit.exec]
    c_args = args.split()
    c_func = dbgen_functor + c_args
    os.chdir(runtime_dir)
    call(c_func)
    os.chdir(mydir)
    database_name = "lineitem.tbl"
    if args[0] != "-h":
        shutil.move(runtime_dir + database_name, mydir + database_name)


class ImportModules:
    def __init__(self, module):
        print("This module is still WIP...")
        self.exec_dir = None
        self.exec = None
        if module == "tpch":
            self.exec_dir = os.environ['TPCH_SUITE'].split(os.pathsep)[0] + "/"
            self.exec = self.exec_dir + "dbgen"
            if self.exec == None:
                sys.exit(
                    "Please set TPCH_SUITE environment variable to your tpch path")
        else:
            print("TODO: Make this intelligent")
            print("Error: You did not install tpch-kit, exiting...")
            assert(False)
