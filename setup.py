"""graphsnapshot
    ~~~~~~~~
    Process links and other data extracted from the Wikipedia dump.

"""

from setuptools import setup, find_packages

setup(
    name='graphsnapshot',
    version='0.0.1',
    author='Cristian Consonni',
    author_email='cris' 'tian.con' 'sonni' '<a' 't>' 'uni' 'tn' '<d' 'ot>it',
    license='GPL3',
    description='Process links and other data extracted from the Wikipedia dump.',
    long_description=__doc__,
    url='https://github.com/CristianCantoro/wikipedia-graphsnapshot',
    packages=find_packages(),
    entry_points={
        'console_scripts': [
            'graphsnapshot=graphsnapshot.__main__:main',
        ],
    },
    install_requires=[
        'arrow==0.7.0',
        'compressed-stream==0.0.2',
        'jsonable==0.3.1',
        'Mako==1.0.2',
        'MarkupSafe==0.23',
        'mediawiki-utilities==0.4.18',
        'mwcites==0.2.0',
        'mwcli==0.0.1',
        'mwparserfromhell==0.4.2',
        'mwtypes==0.2.0',
        'mwxml==0.2.0',
        'regex==2018.8.17',
        'more-itertools==2.2',
        'fuzzywuzzy==0.8.0',
        'python-Levenshtein==0.12.0',
        'requests==2.9.1',
        'typing==3.5.0.1',
    ],
    zip_safe=False,
)
