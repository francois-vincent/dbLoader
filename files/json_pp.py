# -*- coding: utf-8 -*-

__version__ = '0.1.0'
__date__ = 'dec 10th, 2012'
__author__ = 'François Vincent'
__mail__ = 'fvincent@groupeseb.com'
__github__ = 'https://github.com/francois-vincent'


def json_load(filename, mode='eval'):
    """
    using 'eval' mode allows python-style comments and datatypes
    but can lead to coding problems such as unicode-escaped non-ascii chars in strings
    for instance
    """
    with open(filename, 'r') as f:
        if mode == 'json':
            try: import simplejson as json
            except ImportError: import json
            return json.load(f)
        if mode == 'eval':
            return eval(f.read())
        if mode == 'json_eval':
            return eval(f.read().replace('": null', '": None'))
        raise RuntimeError("json_load: '%s': unknown format" % mode)

def json_dump(data, filename, mode='eval'):
    """
    using 'eval' mode allows any python datatype
    """
    with open(filename, 'w') as f:
        if mode == 'json':
            try: import simplejson as json
            except ImportError: import json
            json.dump(data, f, indent=2)
        elif mode == 'eval':
            from pprint import pformat
            f.write(pformat(data))
        elif mode == 'json_eval':
            from pprint import pformat
            f.write(pformat(data).replace('": None', '": null'))
        else:
            raise RuntimeError("json_dump: '%s': unknown format" % mode)


if __name__ == '__main__':
    from pythor import cmdLineExtract
    class cmdLine(cmdLineExtract):
        """
        This application will prettify your ugly json file
        """
        options_aliases = cmdLineExtract.autoShort
        def run(self, source, dest, input_format='json', output_format='json'):
            data = json_load(source, input_format)
            print "converting %d records..."%(len(data),)
            json_dump(data, dest, output_format)
    cmdLine().start()
