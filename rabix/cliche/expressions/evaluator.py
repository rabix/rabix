import os
from xdg.BaseDirectory import save_config_path, xdg_data_dirs
from yapsy.ConfigurablePluginManager import ConfigurablePluginManager
from yapsy.VersionedPluginManager import VersionedPluginManager
from yapsy.PluginManager import PluginManagerSingleton
from yapsy.IPlugin import IPlugin
try:
    from configparser import SafeConfigParser
except ImportError:
    from ConfigParser import SafeConfigParser


class ExpressionEvalPlugin(IPlugin):

    def __init__(self):
        super(ExpressionEvalPlugin, self).__init__()

    def evaluate(self, expression=None, job=None, context=None, *args,
                 **kwargs):
        raise RuntimeError('Not implemented')


class Evaluator(object):

    APP_NAME = 'expression-evaluators'
    _default_dir = 'evaluators'

    def __init__(self, plugin_dir=None):
        self.config = SafeConfigParser()
        config_path = save_config_path(self.APP_NAME)
        self.config_file = os.path.join(config_path, self.APP_NAME + ".conf")
        self.config.read(self.config_file)

        this_dir = os.path.abspath(os.path.dirname(__file__))
        self.plugin_dir = plugin_dir or os.path.join(
            this_dir, self._default_dir)
        places = [self.plugin_dir, ]
        [places.append(os.path.join(path, self.APP_NAME, "evaluators")) for
         path in xdg_data_dirs]

        PluginManagerSingleton.setBehaviour([
            ConfigurablePluginManager,
            VersionedPluginManager,
        ])

        self.manager = PluginManagerSingleton.get()
        self.manager.setConfigParser(self.config, self.write_config)
        self.manager.setPluginInfoExtension("expr-plugin")
        self.manager.setPluginPlaces(places)
        self.manager.collectPlugins()

    def _get_all_evaluators(self):
        return self.manager.getAllPlugins()

    def _get_evaluator(self, name):
        pl = self.manager.getPluginByName(name)
        if not pl:
            raise Exception('No expression evaluator %s' % name)
        return pl.plugin_object

    def write_config(self):
        f = open(self.config_file, "w")
        self.config.write(f)
        f.close()

    def evaluate(self, lang, expression, *args, **kwargs):
        pl = self._get_evaluator(lang)
        return pl.evaluate(expression, *args, **kwargs)
