from setuptools import setup

setup(**{
    'name': 'BeMuch',
    'version': '0.1',
    'author': 'Alexey Poryadin',
    'author_email': 'alexey.poryadin@gmail.com',
    'description': 'This module help to build multiprocessing tornado apps.',
    'py_modules': ['bemuch'],
    'install_requires': [
        'tornado>=4.3',
    ],
    'license': 'MIT'
})
