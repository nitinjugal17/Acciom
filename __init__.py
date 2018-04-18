from utils import App
import os

appInstance = App()

if os.name == 'posix':
    os.system("open -a'Microsoft Excel' {}".format(appInstance.pathname))
