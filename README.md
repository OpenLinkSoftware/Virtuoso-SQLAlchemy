# Virtuoso-SQLAlchemy


A Virtuoso DBMS dialect for SQLAlchemy.

The latest version of this dialect requires SQLAlchemy 1.4 or later.

### Objectives


### Co-requisites

This SQLAlchemy dialect depends on pyodbc (which depends on a working unixODBC environment). This dialect and its dependencies require installation using `pip` oif they are not already in place:

```bash
pip install pyodbc 
pip install sqlalchemy 
pip install virtuoso-sqlalchemy or pip install git+https://github.com/OpenLinkSoftware/Virtuoso-SQLAlchemy.git  
```

### Getting Started

Create an `ODBC DSN (Data Source Name)`_ that points to your target Virtuoso multi-model DBMS instance via `~/.odbc.ini` as exemplified by the following sample snippet:

```ini
; Data Source Name and associated Driver Section

VOS          = OpenLink Virtuoso ODBC Driver (Unicode)

; Data Source Name and associated Driver Library section
[VOS]
Description = Open Virtuoso
Driver      = /usr/local/virtuoso-opensource/lib/virtodbcu_r.so
Database    = Demo
Address     = localhost:1111
WideAsUTF16 = Yes
```
NOTE: 
`WideAsUTF16 = Yes` is mandatory attribute, it is used to transform unicode methods and data in Virtuoso ODBC to UTF16 charset, that is required by unixODBC Driver Manager. 

Most parameters depend on your installation, but be sure to use `virtodbcu_r.so` which comprises [OpenLink Virtuoso ](https://virtuoso.openlinksw.com) 7.2 ODBC driver or Virtuoso 8.x ODBC driver functionality.

Data Source Name (DSN) binding, via SQLAlchemy, occurs via a `virtuoso+pyodbc` scheme URI. 

```python
    from sqlalchemy import create_engine
    engine = create_engine("virtuoso+pyodbc://uid:pwd@your_dsn")
```
Example
```python     
    engine = create_engine("virtuoso+pyodbc://dba:dba@VOS")
```


### The SQLAlchemy Project

SQLAlchemy-access is part of the `SQLAlchemy Project` <https://www.sqlalchemy.org> and adheres to the same standards and conventions as the core project.


### Authors
- 2023, OpenLink Software
- The package based on project https://github.com/maparent/virtuoso-python from `Marc-Antoine Parent`

