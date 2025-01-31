import os
from os.path import join, dirname
from dotenv import load_dotenv

dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)

class env:
    def __getattribute__(self, name):
        return os.environ.get(name)

