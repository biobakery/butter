import os
import sys
import logging
import optparse
import ConfigParser


CONFIG_FILE_KEY = "butter"
ENV_VAR = "BUTTER_CONF"
DEFAULT_CONFIG_LOCATIONS = ["./butter.conf", "/etc/butter/butter.conf"]
if 'HOME' in os.environ:
    DEFAULT_CONFIG_LOCATIONS.insert(1,
        "{}/.config/butter/butter.conf".format(os.environ['HOME'])
    )

class DefaultConfig:
    d = {
        "master_user"     : "repomaster",
        "master_email"    : "schwager@hsph.harvard.edu",
        "autocommit_msg"  : "postrun-autocommit",
        "fatstore"        : "/home/rschwager/fatstore",
        "virtualenv"      : "/home/rschwager/anadama_dev",
        "reporter_url"    : "http://localhost:8082/api/{}",
        "large_file_bytes": 1*1024*1024, # 1MB
        "runner"          : "mrunner",
        "n_runners"       : 1,
        # "partition"     : "general"
    }

    items = lambda self, _: self.d.items()
    get = lambda self, _, key: self.d[key]

default_config = DefaultConfig()


def find_config():
    if ENV_VAR in os.environ:
        return os.environ[ENV_VAR]
    for path in DEFAULT_CONFIG_LOCATIONS:
        if os.path.isfile(path) and os.access(path, os.R_OK):
            return path
    # hope something's been found by now!


def read_config(fname):
    config = ConfigParser.SafeConfigParser()
    try:
        config.read(fname)
    except Exception as e:
        msg = ("Error parsing config file `{}': "+str(e))
        print >> sys.stderr, msg.format(fname)
        sys.exit(1)
    return config


_config = None
def config(fname=None):
    global _config
    if fname is not None:
        _config = read_config(fname)
        return config
    if _config is None:
        fname = find_config()
        if not fname:
            logging.warning("Unable to find configuration file. "
                            "Using built-in defaults")
            _config = default_config
        else:
            _config = read_config(fname)
    return _config


def get(name, cfg=None):
    cfg = cfg or config()
    return cfg.get(CONFIG_FILE_KEY, name)


def config_spew_cmd(argv):
    HELP = "Spew current or default configuration"

    options = [
        optparse.make_option(
            '-d', '--defaults', dest="defaults",
            action="store_true", default=False,
            help=("Spew default configuration parameters instead"
                  " of currently used")),
    ]

    parser = optparse.OptionParser(option_list=options, usage=HELP)
    opts, _ = parser.parse_args(args=argv)

    if opts.defaults:
        items = default_config.items(CONFIG_FILE_KEY)
    else:
        items = config().items(CONFIG_FILE_KEY)

    print "[{}]".format(CONFIG_FILE_KEY)
    for k, v in items:
        print "{} = {}".format(k, v)
