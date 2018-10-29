import re
import ast
from setuptools import setup, find_packages

_version_re = re.compile(r'__version__\s+=\s+(.*)')

with open('redis_fancy_cli/__init__.py', 'rb') as f:
    version = str(ast.literal_eval(_version_re.search(
        f.read().decode('utf-8')).group(1)))

description = 'CLI for REDIS. With auto-completion and syntax highlighting.'

install_requirements = [
    'Click >= 7.0',
    'redis >= 2.10.6',
    'Pygments >= 2.2.0',
    'prompt_toolkit>=2.0.0,<2.1.0',
    'configobj >= 5.0.6',
    # 'humanize >= 0.5.1',
    'cli_helpers >= 1.1.0',
    'six >= 1.11.0'
]

setup(
    name='redis_fancy_cli',
    author='ybrs',
    author_email='i@ybrs.nl',
    version=version,
    license='BSD',
    url='https://github.com/ybrs/redisfancycli',
    packages=find_packages(),
    description=description,
    long_description=open('README.md').read(),
    install_requires=install_requirements,
    entry_points='''
        [console_scripts]
        redis-fancy-cli=redis_fancy_cli.rdscli:main
    ''',
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: Unix',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: SQL',
        'Topic :: Database',
        'Topic :: Database :: Front-Ends',
        'Topic :: Software Development',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
)