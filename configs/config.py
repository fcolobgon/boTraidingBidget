
from dynaconf import Dynaconf
from pathlib import Path
from configs.default import CONFIG_PATH

#print("Path.home() :: "+ str(Path.home()))
#print("BASE_DIR :: "+ str(BASE_DIR))
#print("CONFIG_PATH :: "+ str(CONFIG_PATH))

SETTING_PATH = CONFIG_PATH + "/"

settings = Dynaconf(
    envvar_prefix="DYNACONF",
    settings_files=[SETTING_PATH + 'settings.toml', SETTING_PATH + '.secrets.toml'],
)
