import errno
import shutil
import os
import platform
from os.path import expanduser, exists, dirname
from configobj import ConfigObj
import six
import functools

if six.PY2:
    def makedirs(path):
        try:
            os.makedirs(path)
        except OSError as exc:
            if exc.errno != errno.EEXIST:
                raise
else:
    makedirs = functools.partial(os.makedirs, exist_ok=True)


def ensure_dir_exists(fn):
    def wrapper():
        path = fn()
        makedirs(path)
        return path
    return wrapper


@ensure_dir_exists
def config_location():
    if 'XDG_CONFIG_HOME' in os.environ:
        return '%s/fancy-redis-cli/' % expanduser(os.environ['XDG_CONFIG_HOME'])
    elif platform.system() == 'Windows':
        return os.getenv('USERPROFILE') + '\\AppData\\Local\\dbcli\\fancy-redis-cli\\'
    else:
        return expanduser('~/.config/fancy-redis-cli/')

#
# def load_config(usr_cfg, def_cfg=None):
#     cfg = ConfigObj()
#     cfg.merge(ConfigObj(def_cfg, interpolation=False))
#     cfg.merge(ConfigObj(expanduser(usr_cfg), interpolation=False, encoding='utf-8'))
#     cfg.filename = expanduser(usr_cfg)
#     return cfg
#


def save_default_config(cfg_path):
    cfg = ConfigObj()
    cfg.merge(default_config)
    cfg.filename = cfg_path
    cfg.write()

"""
our default config
"""
default_config = {
    'log_file': '{}/log/redis_fancy_cli.log'.format(config_location()),
    'log_level': 'DEBUG',
    'history_file': '{}/redis_fancy_cli.hist'.format(config_location()),
}

makedirs(dirname(default_config['log_file']))


def get_config(cfg_path=None):

    if not cfg_path:
        cfg_path = os.path.join(config_location(), 'fancy-redis-cli.cfg')
        # we only save default config if its not a custom location
        # user might pass it as a command argument, then we should just raise.
        if not os.path.exists(cfg_path):
            save_default_config(cfg_path)

    return default_config