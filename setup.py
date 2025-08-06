# coding: utf-8
from setuptools import setup, find_packages
from setuptools.extension import Extension
from distutils.extension import Extension
from codecs import open
from os import path
import glob
import re
import sys

from magloader import __version__ as magloader_version
here = path.abspath(path.dirname("__file__"))

with open(path.join(here, "DESCRIPTION.md"), encoding="utf-8") as description:
	description = long_description = description.read()

	name="magloader"
	version = magloader_version

	if sys.version_info.major != 3:
		raise EnvironmentError(f"""{name} requires Python3, and is not compatible with Python2.""")

	setup(
		name=name,
		version=version,
		description=description,
		long_description=long_description,
		url="https://github.com/cschu/magloader",
		author="Christian Schudoma",
		author_email="christian.schudoma@embl.de",
		license="MIT",
		classifiers=[
			"Development Status :: 4 - Beta",
			"Topic :: Scientific/Engineering :: Bio-Informatics",
			"License :: OSI Approved :: MIT License",
			"Operating System :: POSIX :: Linux",
			"Programming Language :: Python :: 3.7",
			"Programming Language :: Python :: 3.8",
			"Programming Language :: Python :: 3.9",
			"Programming Language :: Python :: 3.10",
			"Programming Language :: Python :: 3.11",
			"Programming Language :: Python :: 3.12",
			"Programming Language :: Python :: 3.13",
		],
		install_requires=[
			'lxml',
			'requests',
			'psycopg2-binary',
			"setuptools>=61.0,<69",
			"pymongo",
		],
		zip_safe=False,
		keywords="ena mag upload",
		packages=find_packages(exclude=["test"]),
		package_data={},
		include_package_data=True,
			entry_points={
				"console_scripts": [
					"magloader=magloader.__main__:main",
					"magquery=magloader.query.__main__:main",
					"magprepare=magloader.prepare.__main__:main",
				],
			},
		data_files=[],
	)


# #!/usr/bin/env python
# # -*- coding: UTF-8 -*-
# import os
# from setuptools import setup, find_packages

# setup(
#     name = 'magloader',
#     version='0.1',
#     license='GNU General Public License v3',
#     author='Christian Schudoma',
#     author_email='christian.schudoma@embl.de',
#     description='ENA MAG Uploader',
#     packages=find_packages(exclude=["test"]),
#     package_data={},
#     platforms='any',
#     install_requires=[
#         'lxml',
# 		'requests',
#     ],
#     classifiers=[
#         'Development Status :: 4 - Beta',
#         'Environment :: Web Environment',
#         'Intended Audience :: Developers',
#         'License :: OSI Approved :: GNU General Public License v3',
#         'Operating System :: OS Independent',
#         'Programming Language :: Python',
#         # 'Topic :: Internet :: WWW/HTTP :: Dynamic Content',
#         'Topic :: Software Development :: Libraries :: Python Modules'
#     ],
# 	entry_points={
# 		"console_scripts": [
# 			"magloader=magloader.__main__:main",				
# 		],
# 	},
# 	zip_safe=False,
# 	include_package_data=True,
# 	data_files=[],
# )
