# plugins/auto_install.py
import subprocess

def install_required_libraries():
    packages = ["polars==1.9.0"]
    subprocess.check_call(["pip", "install", *packages])

install_required_libraries()
