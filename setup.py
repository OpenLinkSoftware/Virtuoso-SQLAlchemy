from setuptools import setup, find_packages
import sys
import os

#from pip._internal.download import PipSession
#from pip.req import parse_requirements

#install_reqs = parse_requirements('requirements.txt', session=PipSession())
#requires = [str(ir.req) for ir in install_reqs]

version = '0.1.0'

readme = os.path.join(os.path.dirname(__file__), "README.md")



setup(name="virtuoso-sqlalchemy",
    version=version,
    description="OpenLink Virtuoso Support for SQLAlchemy",
    long_description=open(readme).read(),
    long_description_content_type="text/markdown",
    url="https://github.com/OpenLinkSoftware/Virtuoso-SQLAlchemy",
#    url='http://packages.python.org/virtuoso-sqlalchemy',
    author='OpenLink Software',
    author_email='support@openlinksw.com',
    license='BSD',
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: Implementation :: CPython",
        "Topic :: Database :: Front-Ends",
        "Operating System :: OS Independent",
    ],
    keywords="SQLAlchemy OpenLink Virtuoso",
    project_urls={
#        "Documentation": "https://github.com/OpenLinkSoftware/Virtuoso-SQLAlchemy/docs",
        "Source": "https://github.com/OpenLinkSoftware/Virtuoso-SQLAlchemy",
        "Tracker": "https://github.com/OpenLinkSoftware/Virtuoso-SQLAlchemy/issues",
    },
    packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
    include_package_data=True,
    install_requires=["SQLAlchemy>=1.4.0", "pyodbc>=4.0.27"],
    zip_safe=False,
    tests_require=["nose"],
    entry_points={
        "sqlalchemy.dialects": [
            "virtuoso.pyodbc = virtuoso.alchemy:VirtuosoDialect"
        ]
    },
)
