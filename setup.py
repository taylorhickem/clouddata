from setuptools import setup, find_packages


setup(
    name='clouddata',
    version=open('VERSION').read(),
    packages=find_packages(),
    url='https://github.com/taylorhickem/clouddata.git',
    description='integration between different cloud data API services',
    author='@taylorhickem',
    long_description=open('README.md').read(),
    install_requires=open('requirements.txt', 'r').read().splitlines(),
    include_package_data=True
)
