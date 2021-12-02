from setuptools import setup

with open("README.md", "r") as readme:
    long_description = readme.read()


setup(
    name='gridmet',
    version="0.0.1.0",
    url='https://gitlab-int.rc.fas.harvard.edu/rse/francesca_dominici/tools/gridmet',
    license='',
    author='Michael Bouzinier',
    author_email='mbouzinier@g.harvard.edu',
    description='EPA Data Pipelines',
    long_description = long_description,
    long_description_content_type = "text/markdown",
    #py_modules = [''],
    package_dir={
        "gridmet": "./src/python/gridmet"
    },
    packages=["gridmet"],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: Harvard University :: Development",
        "Operating System :: OS Independent"],
    install_requires=[
        'geopandas',
        'geopy',
        'h5py',
        'netCDF4',
        'numpy',
        'pandas',
        'psutil',
        'pygeos',
        'pyshp',
        'rasterstats',
        'rasterio >= 1.1.0',
        'requests',
        'nsaph_utils >= 0.0.5.2',
        'nsaph>=0.0.2.0'
    ],
    package_data = {
        '': ["**/*.yaml"]
    }
)
