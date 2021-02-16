from setuptools import setup

setup(
    name='pydxhealthcheck',
    version='1.0',
    py_modules=['pydxhealthcheck'],
    install_requires=[
        'Click',
        'matplotlib==2.2.4',
        'numpy==1.20.1',
        'pandas==0.24.2',
        'pytz==2018.9',
        'python-pptx==0.6.17'
    ],
    entry_points='''
        [console_scripts]
        pydxhealthcheck=pydxhealthcheck:cli
    ''',
)
