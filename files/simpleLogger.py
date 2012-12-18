# -*- coding: utf-8 -*-

__version__ = '0.2'
__date__ = 'nov 16th, 2012'
__author__ = 'François Vincent'
__mail__ = 'francois.vincent01@gmail.com'

import sys, os, time, traceback

import logging
INFO = logging.INFO
WARNING = logging.WARNING
ERROR = logging.ERROR
CRITICAL = logging.CRITICAL
color_green = '\033[92m'
color_yellow = '\033[93m'
color_red = '\033[91m'
color_crit = '\033[95m'
color_endc = '\033[0m'

loggersDict = {}
logFormat =   '%(asctime)s:%(levelname)-8s(%(lineno)4d)[%(message)s]'
colorFormat = '%(asctime)s:%(levelname)-8s[%(color)s%(message)s%(endc)s]'
firstcall = True

def _getLogger(name='root'):
	return logging.Logger.manager.getLogger(name)
logging.getLogger = _getLogger

class autoLogger(logging.Logger):
	def __enter__(self):
		return self
	def __exit__(self, exc_type, exc_val, exc_tb):
		if exc_type:
			module, lineno = traceback.extract_tb(exc_tb, 1)[0][0:2]
			loc = '%s (%d) '%(module, lineno)
			error = traceback.format_exception(exc_type, exc_val, exc_tb, 1)[-1].strip()
			self.critical(loc+error)

class colorLogger(autoLogger):
	def info(self, msg, *args, **kwargs):
		kwargs['extra'] = dict(color='', endc='')
		if self.isEnabledFor(INFO):
			self._log(INFO, msg, args, **kwargs)
	def warning(self, msg, *args, **kwargs):
		kwargs['extra'] = dict(color=color_yellow, endc=color_endc)
		if self.isEnabledFor(WARNING):
			self._log(WARNING, msg, args, **kwargs)
	def error(self, msg, *args, **kwargs):
		kwargs['extra'] = dict(color=color_red, endc=color_endc)
		if self.isEnabledFor(ERROR):
			self._log(ERROR, msg, args, **kwargs)
	def critical(self, msg, *args, **kwargs):
		kwargs['extra'] = dict(color=color_crit, endc=color_endc)
		if self.isEnabledFor(CRITICAL):
			self._log(CRITICAL, msg, args, **kwargs)
logging.setLoggerClass(autoLogger)

def getLogger(filename=None, level=INFO, screen=None, prefix=None, color=None, policy=None):
	global firstcall
	if not filename:
		# define filename from name of topmost python file
		filename = os.path.basename(traceback.extract_stack()[0][0]).split('.')[0]
	if filename in loggersDict:
		return logging.getLogger(loggersDict[filename])
	if firstcall:
		# this allows for logging.getLogger() to return logger created by 1st call
		# to autoLogger.getLogger() so that logging in application submodules can follow a standard
		# logging scheme (without names) while being still compatible with autoLogger
		loggerName = 'root'
		firstcall = False
	else:
		loggerName = filename
	loggersDict[filename] = loggerName
	filename += time.strftime('_%Y-%m-%d.log')
	prefix_ok = True
	if prefix:
		filename = os.path.join(prefix, filename)
		try:
			if not os.path.exists(prefix):
				os.makedirs(prefix)
			if not os.access(prefix, os.W_OK):
				prefix_ok = False
		except OSError:
			prefix_ok = False
	if color:
		logging.setLoggerClass(colorLogger)
	logger = logging.getLogger(loggerName)
	logging.setLoggerClass(autoLogger)
	logger.propagate = False
	logger.setLevel(logging.INFO)
	if prefix_ok:
		fh = logging.FileHandler(filename)
		fh.setLevel(level)
		fh.setFormatter(logging.Formatter())
		logger.addHandler(fh)
		logger.error('*'*35+' getLogger log opened on %s '%time.asctime()+'*'*35)
		fh.setFormatter(logging.Formatter(logFormat))
	else:
		print >>sys.stderr, "ERROR: Could not write in log file", filename
	if screen:
		sh = logging.StreamHandler()
		if color:
			sh.setFormatter(logging.Formatter(colorFormat))
		else:
			sh.setFormatter(logging.Formatter(logFormat))
		sh.setLevel(screen)
		logger.addHandler(sh)
	return logger
