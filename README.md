# Virtuoso-SQLAlchemy


A Virtuoso DBMS dialect for SQLAlchemy.

The latest version of this dialect requires SQLAlchemy 1.4 or later.

### Objectives


### Co-requisites

This SQLAlchemy dialect depends on `pyodbc` (which depends on a working `unixODBC` environment). If they are not already in place, this dialect and its dependencies require installation using `pip` with commands like those below:

1. ```bash
   pip install pyodbc 
   ```
2. ```bash
   pip install sqlalchemy 
   ```
3. ```bash
   pip install virtuoso-sqlalchemy
   ```
   or
   ```bash
   pip install git+https://github.com/OpenLinkSoftware/Virtuoso-SQLAlchemy.git  
   ```

### Getting Started

Create an ODBC DSN (Data Source Name) that points to your target Virtuoso multi-model DBMS instance via `~/.odbc.ini`, as in the following sample:

```ini
; Data Source Name and associated Driver Section
; usually titled [ODBC Data Sources]
VOS          = OpenLink Virtuoso ODBC Driver (Unicode)

; Data Source Name and associated Driver Library section
[VOS]
Description = OpenLink Virtuoso
Driver      = /usr/local/virtuoso-opensource/lib/virtodbcu_r.so
Database    = Demo
Address     = localhost:1111
WideAsUTF16 = Yes
```
NOTE: 
`WideAsUTF16 = Yes` is a mandatory attribute. It is used to transform Unicode methods and data in Virtuoso ODBC to the UTF16 character set, as is required by the `unixODBC` Driver Manager. 

Most parameters depend on your installation, but be sure to use `virtodbcu_r.so` which comprises [OpenLink Virtuoso ](https://virtuoso.openlinksw.com) 7.2 ODBC driver or Virtuoso 8.x ODBC driver functionality.

Via SQLAlchemy, DSN binding occurs via a `virtuoso+pyodbc` scheme URI. 

```python
from sqlalchemy import create_engine
engine = create_engine("virtuoso+pyodbc://uid:pwd@your_dsn")
```
Example
```python     
engine = create_engine("virtuoso+pyodbc://dba:dba@VOS")
```


### The SQLAlchemy Project

SQLAlchemy-access is part of the [**SQLAlchemy Project**](https://www.sqlalchemy.org) and adheres to the same standards and conventions as the core project.


### Authors
- © 2023, OpenLink Software, Inc
- This package is based on the [`virtuoso-python`](https://github.com/maparent/virtuoso-python) project from [`Marc-Antoine Parent`](https://github.com/maparent/)

