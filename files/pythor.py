# -*- coding: utf-8 -*-

__version__ = '0.1.4'
__date__ = 'oct 17th, 2012'
__author__ = 'François Vincent'
__mail__ = 'francois.vincent01@gmail.com'

import sys, inspect, shlex

class autoShort:
    add = None
    def __add__(self, other):
        self.add = other
        return self

class cmdLineExtract:
    '''
    This virtual class extracts args off the run() method of any derived subclass
    Then it extracts cmd line arguments and matches them to run() args, then launches run()
    '''
    _help_options = set(('-h', '--help', '-?'))
    _kwargs_key_pat = '__kw__%d'
    options_aliases = None
    autoShort = autoShort
    def __init__(self):
        argspec = inspect.getargspec(self.run)
        # get the ** kwarg name or None
        self.kwargs = argspec.keywords
        # get required args as a list and optional args as a dict (with default values)
        if argspec.defaults:
            self.reqargs = argspec.args[1:-len(argspec.defaults)]
            options = dict(zip(argspec.args[-len(argspec.defaults):], argspec.defaults))
        else:
            self.reqargs = argspec.args[1:]
            options = {}
        # make an equivalence dict from line cmd style (--file-name) to python style (file_name) args
        self.options_equ = dict([('-'+x if len(x)==1 else '--'+'-'.join(x.split('_')), x) for x in options])
        # make a dict of cmd line style arg names to their types
        self.options = dict([('-'+x if len(x)==1 else '--'+'-'.join(x.split('_')), options[x]) for x in options])
        # take a copy of original (no aliases yet) optional args for print_help()
        self._options = dict(self.options)
        # create automatic short options from long options
        if self.options_aliases is autoShort or isinstance(self.options_aliases, autoShort):
            options_aliases = {}
            for k in argspec.args[-len(argspec.defaults):]:
                if len(k) > 1:
                    options_aliases[k] = (k[0],)
            if hasattr(self.options_aliases, 'add') and self.options_aliases.add:
                options_aliases.update(self.options_aliases.add)
            self.options_aliases = options_aliases
        # inject aliases into dicts
        if self.options_aliases:
            for x, t in options.items():
                if x in self.options_aliases:
                    for a in self.options_aliases[x]:
                        k = '-'+a
                        if k in self.options:
                            raise RuntimeError("short option '%s' redefined" % k)
                        self.options[k] = t
                        self.options_equ[k] = x
    def start(self, param_string=None):
        """
        this is the function you call to run your command
        param_string is defaulted to the actual command line
        otherwise you can specify your own if you like :))
        """
        allargs = {}
        reqargs, reqargs_index = {}, 0
        optargs, kwargs_index = {}, 0
        # first, search cmd line options and their args
        # and collect them in allargs.
        # along this process, required and optional args are also collected
        # in reqargs and optargs
        if param_string:
            argv = shlex.split(param_string)
        else:
            argv = sys.argv[1:]
        i = 0
        while i<len(argv):
            x = argv[i]
            if x in self._help_options:
                self._print_help()
                return True
            if x in self.options:
                if self.options_equ[x] in allargs:
                    print >>sys.stderr, "option '%s' found twice" % x
                    return
                if isinstance(self.options[x], list):
                    argpos = i
                    allargs[self.options_equ[x]] = []
                    i += 1
                    while i<len(argv) and argv[i] not in self.options:
                        try:
                            allargs[self.options_equ[x]].append(type(self.options[x][0])(argv[i]))
                        except ValueError:
                            print >>sys.stderr, "argument %d of option %s has wrong type (%s expected)"%\
                                                                    (i-argpos, x, type(self.options[x][0]))
                            return
                        i += 1
                    if not len(allargs[self.options_equ[x]]):
                        print >>sys.stderr, "option '%s' should be followed by a list of %s"%\
                                                                (x, type(self.options[x][0]))
                        return
                elif type(self.options[x]) is bool:
                    allargs[self.options_equ[x]] = True
                    i += 1
                else:
                    i += 1
                    if i>=len(argv) or argv[i] in self.options:
                        print >>sys.stderr, "option '%s' should be followed by a %s"%\
                                                                (x, type(self.options[x]))
                        return
                    try:
                        allargs[self.options_equ[x]] = type(self.options[x])(argv[i])
                    except ValueError:
                        print >>sys.stderr, "argument of option %s has wrong type (%s expected)"%\
                                                                (x, type(self.options[x]))
                        return
                    i += 1
            else:
                if len(reqargs) < len(self.reqargs):
                    reqargs[self.reqargs[reqargs_index]] = argv[i]
                    reqargs_index += 1
                    i += 1
                elif self.kwargs:
                    optargs[self._kwargs_key_pat%kwargs_index] = argv[i]
                    kwargs_index += 1
                    i += 1
                else:
                    print >>sys.stderr, "unrecognized option '%s'" % argv[i]
                    return
        if len(reqargs) < len(self.reqargs):
            print >>sys.stderr, "Too few required parameters (%d specified)" % len(self.reqargs)
            return
        # merge required and optional args in allargs
        allargs.update(reqargs)
        allargs.update(optargs)
        self.run(**allargs)
        return True
    def start_batch(self, param_list, verbosity=2, stop_on_error=True):
        "use this to run a sequence of parameters strings"
        for i, param_string in enumerate(param_list, 1):
            if verbosity:
                text = "** Process line %d: <%s>" % (i, param_string)
                if verbosity > 1:
                    print '\n ', text, '\n ', '-'*len(text)
                else:
                    print ' ', text
            if not self.start(param_string) and stop_on_error:
                print >>sys.stderr, "could not process string <%s>, aborted at line %d" % (param_string, i)
                return
        return True
    def convert_kwargs_to_tuple(self, kw):
        """you have to call this to get your optional parameters list"""
        return tuple([kw[self._kwargs_key_pat%i] for i in xrange(len(kw))])
    def _print_help(self):
        """
        Help is automatically generated from the __doc__ of the subclass
        and from the names of the args of run(). Therefore args names selection
        is more important than ever here !
        """
        print '\n ', inspect.getfile(self.__class__), ' '.join(self.reqargs),
        if self.kwargs:
            print '[%s]' % self.kwargs,
        if self.options:
            print '[options]',
        print
        if self.__doc__:
            import textwrap
            print '\n', textwrap.fill(textwrap.dedent(self.__doc__).strip(), width=80, initial_indent='  ', subsequent_indent='  '), '\n'
        if self.options:
            print 'Options:'
            for x, t in self._options.items():
                print x,
                if self.options_aliases and self.options_equ[x] in self.options_aliases:
                    print ' | -'+' | -'.join(self.options_aliases[self.options_equ[x]]),
                if isinstance(t, list):
                    print '<list of %s>' % type(t[0]),
                elif not isinstance(t, bool):
                    print type(t),
                print '(default=%r)'%t
    def run(self):
        raise NotImplementedError('run must be implemented '
                                                          'by cmdLineExtract subclass')

if __name__ == '__main__':
    # Example subclass.
    # Tested types are str(), int(), bool(), float() and [] of them.
    # Tested features are: required parameters and different options types.
    # Optional parameters are not tested here.
    # Example uses:
    tests = [
            '-h',
            'fic1 fic2',
            'fic1 fic2 --toto 123',
            'fic1 fic2 --toto 123x',
            'fic1 fic2 -to 123 -ti "python is wahou" -c',
#            '--cond fic1 fic2 -ll 10 20 30 -d 10',
#            '--cond fic1 -ll 10 20 30 fic2 -d 10',
#            '--cond fic1 -lu 10 20 30 --toto 5 -d 10 fic2',
#            '--cond fic1 -ll 10 20 30 --toto 5 -d 10 fic2 fic3',
#            '--cond fic1 -ll 10 20 30 --toto 5 -d 10',
#            '--cond fic1 -ll 10 20 30x --toto 5 -d 10 fic2',
#            '--cond fic1 -ll 10 20 30 --toto 5 -d',
    ]
    class myoptions(cmdLineExtract):
        "This is a simple example of the use of module pythor.py"
        options_aliases = {
        'toto' : ('to', ),
        'titi' : ('ti', ),
        'lulu_lulu' : ('ll', 'lu'),
        'cond' : ('c', ),
        }
        def run(self, file1, file2, toto=int(), titi=str(), lulu_lulu=[int()], cond=bool(), d=float()):
            print "filenames", file1, file2
            print "toto=<%r>, titi=<%r>, lulu_lulu=<%r>, cond=<%r>, d=<%r>" % (toto, titi, lulu_lulu, cond, d)

    myoptions().start_batch(tests, stop_on_error=False)
