application_title = "Acciom"
main_python_file = "__init__.py"

import sys

from cx_Freeze import setup, Executable

base = None
if sys.platform == "win32":
    base = "Win32GUI"

includes = ["re"]

setup(
        name = application_title,
        version = "0.1",
        description = "Acciom : Data Analysis Tool ",
        options = {"build_exe" : {"includes" : includes }},
        executables = [Executable(main_python_file, base = base)])