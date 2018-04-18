from utils import App
import os
import sys

appInstance = App()

sys.stdout = open('stdout.txt', 'w')
if os.name == 'posix':
    os.system("open -a'Microsoft Excel' {}".format(appInstance.pathname))
