# Virtuoso-SQLAlchemy


A Virtuoso DBMS dialect for SQLAlchemy.

The latest version of this dialect requires SQLAlchemy 1.4 or later.

### Objectives


### Co-requisites

This dialect requires SQLAlchemy, pyodbc and unixODBC. They are specified as requirements so `pip`
will install them if they are not already in place. To install, just::

```bash
pip install virtuoso-sqlalchemy
```
Or
```
pip install git+https://github.com/OpenLinkSoftware/Virtuoso-SQLAlchemy.git  
```

### Getting Started

Create an `ODBC DSN (Data Source Name)`_ that points to your Virtuoso database.
You have to set up your `~/.odbc.ini` (or `/etc/odbc.ini`) file with a block similar to this:

```ini
[VOS]
Description = Open Virtuoso
Driver      = /usr/local/virtuoso-opensource/lib/virtodbcu_r.so
Database    = Demo
Address     = localhost:1111
WideAsUTF16 = Yes
```
NOTE: `WideAsUTF16 = Yes` is mandatory attribute, it is switch unicode methods and data in Virtuoso ODBC to UTF16 charset, that is required for unixODBC DM. 
Most parameters depend on your installation, but be sure to use `virtodbcu_r.so`. 
Use the latest version of Virtuoso 7.2 ODBC driver or Virtuoso 8.x ODBC driver.

[OpenLink Virtuoso ](https://virtuoso.openlinksw.com) 
```python
    from sqlalchemy import create_engine
    engine = create_engine("virtuoso+pyodbc://uid:pwd@your_dsn")
```
Sample
```python     
    engine = create_engine("virtuoso+pyodbc://dba:dba@VOS")
```


### The SQLAlchemy Project

SQLAlchemy-access is part of the `SQLAlchemy Project` <https://www.sqlalchemy.org> and adheres to the same standards and conventions as the core project.


### Authors
2023, OpenLink Software
The package based on project https://github.com/maparent/virtuoso-python from `Marc-Antoine Parent`

