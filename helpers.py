import os
import shutil

def recreate_dir(dir):
    shutil.rmtree(dir, ignore_errors=True)
    if not os.path.exists(dir):
        os.makedirs(dir)