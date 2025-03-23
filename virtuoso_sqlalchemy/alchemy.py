assert __import__("pkg_resources").get_distribution(
    "sqlalchemy").version.split('.') >= ['1', '4'], \
    "requires sqlalchemy version 1.4 or greater"

from collections import defaultdict
from builtins import next
import warnings
from datetime import datetime

#from werkzeug.urls import iri_to_uri
from sqlalchemy import schema, Table, exc, util, types
from sqlalchemy.schema import Constraint
from sqlalchemy.sql import (text, bindparam, compiler, operators)
from sqlalchemy.sql.expression import (
    BindParameter, TextClause, cast, ColumnElement)
from sqlalchemy.sql.schema import Sequence
from sqlalchemy.sql.compiler import BIND_PARAMS, BIND_PARAMS_ESC
from sqlalchemy.sql.ddl import _CreateDropBase
from sqlalchemy.connectors.pyodbc import PyODBCConnector
from sqlalchemy.engine import default
from sqlalchemy.sql.functions import GenericFunction
from sqlalchemy.types import (
    CHAR, VARCHAR, TIME, NCHAR, NVARCHAR, DATETIME, FLOAT, String, NUMERIC,
    INTEGER, SMALLINT, VARBINARY, DECIMAL, TIMESTAMP, UnicodeText, REAL,
    Unicode, Text, Float, LargeBinary, UserDefinedType, TypeDecorator)
from sqlalchemy.orm import column_property
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.sql.elements import Grouping, ClauseList
from sqlalchemy.engine import reflection

# import past.builtins
import pyodbc


class VirtuosoExecutionContext(default.DefaultExecutionContext):
    def get_lastrowid(self):
        # Experimental and unreliable
        # if self.cursor.lastserial > 0:
        #     return self.cursor.lastserial
        self.cursor.execute("SELECT identity_value() AS lastrowid")
        lastrowid = int(self.cursor.fetchone()[0])
        #print "idvalue: %d, lser: %d" % (lastrowid, self.cursor.lastserial)
        return lastrowid

    def fire_sequence(self, seq, type_):
        return self._execute_scalar((
            "select sequence_next('%s')" %
            self.dialect.identifier_preparer.format_sequence(seq)), type_)


class VirtuosoSequence(Sequence):
    def upcoming_value(self, connection):
        # This gives the upcoming value without advancing the sequence
        preparer = connection.bind.dialect.identifier_preparer
        (val,) = next(iter(connection.execute(
            "SELECT sequence_set('%s', 0, 1)" %
            (preparer.format_sequence(self),))))
        return int(val)

    def set_value(self, value, connection):
        preparer = connection.bind.dialect.identifier_preparer
        connection.execute(
            "SELECT sequence_set('%s', %d, 0)" %
            (preparer.format_sequence(self), value))


RESERVED_WORDS = {
    '__cost', '__elastic', '__tag', '__soap_doc', '__soap_docw',
    '__soap_header', '__soap_http', '__soap_name', '__soap_type',
    '__soap_xml_type', '__soap_fault', '__soap_dime_enc', '__soap_enc_mime',
    '__soap_options', 'ada', 'add', 'admin', 'after', 'aggregate', 'all',
    'alter', 'and', 'any', 'are', 'array', 'as', 'asc', 'assembly', 'attach',
    'attribute', 'authorization', 'autoregister', 'backup', 'before', 'begin',
    'best', 'between', 'bigint', 'binary', 'bitmap', 'breakup', 'by', 'c',
    'call', 'called', 'cascade', 'case', 'cast', 'char', 'character', 'check',
    'checked', 'checkpoint', 'close', 'cluster', 'clustered', 'clr',
    'coalesce', 'cobol', 'collate', 'column', 'commit', 'committed',
    'compress', 'constraint', 'constructor', 'contains', 'continue', 'convert',
    'corresponding', 'create', 'cross', 'cube', 'current', 'current_date',
    'current_time', 'current_timestamp', 'cursor', 'data', 'date', 'datetime',
    'decimal', 'declare', 'default', 'delete', 'desc', 'deterministic',
    'disable', 'disconnect', 'distinct', 'do', 'double', 'drop', 'dtd',
    'dynamic', 'else', 'elseif', 'enable', 'encoding', 'end', 'escape',
    'except', 'exclusive', 'execute', 'exists', 'external', 'extract', 'exit',
    'fetch', 'final', 'float', 'for', 'foreach', 'foreign', 'fortran',
    'for_vectored', 'for_rows', 'found', 'from', 'full', 'function', 'general',
    'generated', 'go', 'goto', 'grant', 'group', 'grouping', 'handler',
    'having', 'hash', 'identity', 'identified', 'if', 'in', 'incremental',
    'increment', 'index', 'index_no_fill', 'index_only', 'indicator', 'inner',
    'inout', 'input', 'insert', 'instance', 'instead', 'int', 'integer',
    'intersect', 'internal', 'interval', 'into', 'is', 'isolation', 'iri_id',
    'iri_id_8', 'java', 'join', 'key', 'keyset', 'language', 'left', 'level',
    'library', 'like', 'locator', 'log', 'long', 'loop', 'method', 'modify',
    'modifies', 'module', 'mumps', 'name', 'natural', 'nchar', 'new',
    'nonincremental', 'not', 'no', 'novalidate', 'null', 'nullif', 'numeric',
    'nvarchar', 'object_id', 'of', 'off', 'old', 'on', 'open', 'option', 'or',
    'order', 'out', 'outer', 'overriding', 'partition', 'pascal', 'password',
    'percent', 'permission_set', 'persistent', 'pli', 'position', 'precision',
    'prefetch', 'primary', 'privileges', 'procedure', 'public', 'purge',
    'quietcast', 'rdf_box', 'read', 'reads', 'real', 'ref', 'references',
    'referencing', 'remote', 'rename', 'repeatable', 'replacing',
    'replication', 'resignal', 'restrict', 'result', 'return', 'returns',
    'revoke', 'rexecute', 'right', 'rollback', 'rollup', 'role', 'safe',
    'same_as', 'uncommitted', 'unrestricted', 'schema', 'select', 'self',
    'serializable', 'set', 'sets', 'shutdown', 'smallint', 'snapshot', 'soft',
    'some', 'source', 'sparql', 'specific', 'sql', 'sqlcode', 'sqlexception',
    'sqlstate', 'sqlwarning', 'static', 'start', 'style', 'sync', 'system',
    't_cycles_only', 't_direction', 't_distinct', 't_end_flag', 't_exists',
    't_final_as', 't_in', 't_max', 't_min', 't_no_cycles', 't_no_order',
    't_out', 't_shortest_only', 'table', 'temporary', 'text', 'then', 'ties',
    'time', 'timestamp', 'to', 'top', 'type', 'transaction', 'transitive',
    'trigger', 'under', 'union', 'unique', 'update', 'use', 'user', 'using',
    'validate', 'value', 'values', 'varbinary', 'varchar', 'variable',
    'vector', 'vectored', 'view', 'when', 'whenever', 'where', 'while', 'with',
    'without', 'work', 'xml', 'xpath'}


class VirtuosoIdentifierPreparer(compiler.IdentifierPreparer):
    reserved_words = RESERVED_WORDS

    def quote_schema(self, schema, force=None):
        if '.' in schema:
            cat, schema = schema.split('.', 1)
            return self.quote(cat, force) + '.' + self.quote(schema, force)
        else:
            return self.quote(schema, force)

    def format_sequence(self, sequence, use_schema=True):
        res = super(VirtuosoIdentifierPreparer, self).format_sequence(
            sequence, use_schema=use_schema)
        # unquote
        return res.strip('"')


class VirtuosoSQLCompiler(compiler.SQLCompiler):
    ansi_bind_rules = True
    extract_map = {
        'day': 'dayofmonth(%s)',
        'dow': 'dayofweek(%s)',
        'doy': 'dayofyear(%s)',
        'epoch': 'msec_time()',
        'hour': 'hour(%s)',
        'microseconds': '0',
        'milliseconds': 'atoi(substring(datestring(%s), 20, 6))',
        'minute': 'minute(%s)',
        'month': 'month(%s)',
        'quarter': 'quarter(%s)',
        'second': 'second(%s)',
        'timezone_hour': 'floor(timezone(%s)/60)',
        'timezone_minute': 'mod(timezone(%s),60)',
        'week': 'week(%s)',
        'year': 'year(%s)'
    }

    def get_select_precolumns(self, select, **kw):
        s = select._distinct and "DISTINCT " or ""
        # TODO: check if Virtuoso supports
        # bind params for FIRST / TOP
        if select._limit or select._offset:
            if select._offset:
                limit = select._limit or '100000'
                s += "TOP %s, %s " % (select._offset, limit)
            else:
                s += "TOP %s " % (select._limit,)
        return s

    def limit_clause(self, select, **kw):
        # Limit in virtuoso is after the select keyword
        return ""

    def visit_now_func(self, fn, **kw):
        return "GETDATE()"

    def visit_sequence(self, seq):
        return "sequence_next('%s')" % self.preparer.format_sequence(seq)

    def visit_extract(self, extract, **kw):
        func = self.extract_map.get(extract.field)
        if not func:
            raise exc.CompileError(
                "%s is not a valid extract argument." % extract.field)
        return func % (self.process(extract.expr, **kw), )

    def visit_true(self, expr, **kw):
        return '1'

    def visit_false(self, expr, **kw):
        return '0'

    def visit_in_op_binary(self, binary, operator, **kw):
        """ This is beyond absurd. Virtuoso gives weird results on other columns
        when doing a single-value IN clause. Avoid those. """
        if (isinstance(binary.right, Grouping)
                and isinstance(binary.right.element, ClauseList)
                and len(binary.right.element.clauses) == 1):
            el = binary.right.element.clauses[0]
            return "%s = %s" % (
                self.process(binary.left, **kw),
                self.process(el, **kw))
        return self._generate_generic_binary(binary, " IN ", **kw)

    def visit_binary(self, binary, **kwargs):
        if binary.operator == operators.ne:
            if isinstance(binary.left, BindParameter)\
                    and isinstance(binary.right, BindParameter):
                kwargs['literal_binds'] = True
            return self._generate_generic_binary(
                binary, ' <> ', **kwargs)

        return super(VirtuosoSQLCompiler, self).visit_binary(binary, **kwargs)

    def render_literal_value(self, value, type_):
        if isinstance(value, IRI_ID_Literal):
            return value
        return super(VirtuosoSQLCompiler, self)\
            .render_literal_value(value, type_)

    def visit_sparqlclause(self, sparqlclause, **kw):
        def do_bindparam(m):
            name = m.group(1)
            if name in sparqlclause._bindparams:
                self.process(sparqlclause._bindparams[name], **kw)
            return '??'

        # un-escape any \:params
        text = BIND_PARAMS_ESC.sub(
            lambda m: m.group(1),
            BIND_PARAMS.sub(
                do_bindparam,
                self.post_process_text(sparqlclause.text))
        )
        if sparqlclause.quad_storage:
            text = 'define input:storage %s %s' % (
                sparqlclause.quad_storage, text)
        return 'SPARQL ' + text


class SparqlClause(TextClause):
    __visit_name__ = 'sparqlclause'

    def __init__(self, text, bind=None, quad_storage=None):
        super(SparqlClause, self).__init__(text, bind)
        self.quad_storage = quad_storage

    def columns(self, *cols, **types):
        textasfrom = super(SparqlClause, self).columns(*cols, **types)
        return textasfrom.alias()


class LONGVARCHAR(Text):
    __visit_name__ = 'LONG_VARCHAR'


class LONGNVARCHAR(UnicodeText):
    __visit_name__ = 'LONG_NVARCHAR'


class DOUBLEPRECISION(Float):
    __visit_name__ = 'DOUBLE_PRECISION'


class LONGVARBINARY(LargeBinary):
    __visit_name__ = 'LONG_VARBINARY'



# class _cast_nvarchar(ColumnElement):
#    def __init__(self, bindvalue):
#        self.bindvalue = bindvalue


# @compiles(_cast_nvarchar)
# def _compile(element, compiler, **kw):
#    return compiler.process(cast(element.bindvalue, Unicode), **kw)


class dt_set_tz(GenericFunction):
    "Convert IRI IDs to int values"
    type = DATETIME
    name = "dt_set_tz"

    def __init__(self, adatetime, offset, **kw):
        if not (isinstance(adatetime, (datetime, DATETIME))
                or isinstance(adatetime.__dict__.get('type'), DATETIME)):
            warnings.warn(
                "dt_set_tz() accepts a DATETIME object as first input.")
        if not (isinstance(offset, (int, INTEGER))
                or isinstance(offset.__dict__.get('type'), INTEGER)):
            warnings.warn(
                "dt_set_tz() accepts a INTEGER object as second input.")
        super(dt_set_tz, self).__init__(adatetime, offset, **kw)


class Timestamp(TypeDecorator):
    impl = TIMESTAMP
    # Maybe TypeDecorator should delegate? Another story
    python_type = datetime

    def column_expression(self, colexpr):
        return dt_set_tz(cast(colexpr, DATETIME), 0)

#     def bind_expression(self, bindvalue):
#         return _cast_timestamp(bindvalue)


# class _cast_timestamp(ColumnElement):
#     def __init__(self, bindvalue):
#         self.bindvalue = bindvalue


# @compiles(_cast_timestamp)
# def _compile(element, compiler, **kw):
#     return compiler.process(cast(element.bindvalue, DATETIME), **kw)



TEXT_TYPES = (CHAR, VARCHAR, NCHAR, NVARCHAR, String, UnicodeText,
              Unicode, Text, LONGVARCHAR, LONGNVARCHAR)


class IRI_ID_Literal(str):
#    "An internal virtuoso IRI ID, of the form #innnnn"
    def __str__(self):
        return 'IRI_ID_Literal("%s")' % (self, )

    def __repr__(self):
        return str(self)


class IRI_ID(UserDefinedType):
    "A column type for IRI ID"
    __visit_name__ = 'IRI_ID'

    def __init__(self):
        super(IRI_ID, self).__init__()

    def get_col_spec(self):
        return "IRI_ID"

    def bind_processor(self, dialect):
        def process(value):
            if value:
                return IRI_ID_Literal(value)
        return process

    def result_processor(self, dialect, coltype):
        def process(value):
            if value:
                return IRI_ID_Literal(value)
        return process


class iri_id_num(GenericFunction):
    "Convert IRI IDs to int values"
    type = INTEGER
    name = "iri_id_num"

    def __init__(self, iri_id, **kw):
        if not isinstance(iri_id, IRI_ID_Literal)\
                and not isinstance(iri_id.__dict__.get('type'), IRI_ID):
            warnings.warn("iri_id_num() accepts an IRI_ID object as input.")
        super(iri_id_num, self).__init__(iri_id, **kw)


class iri_id_from_num(GenericFunction):
    "Convert numeric IRI IDs to IRI ID literal type"
    type = IRI_ID
    name = "iri_id_from_num"

    def __init__(self, num, **kw):
        if not isinstance(num, int):
            warnings.warn("iri_id_num() accepts an Integer as input.")
        super(iri_id_from_num, self).__init__(num, **kw)


class id_to_iri(GenericFunction):
    "Get the IRI from a given IRI ID"
    type = String
    name = "id_to_iri"

    def __init__(self, iri_id, **kw):
        # TODO: Handle deferred.
        if not isinstance(iri_id, IRI_ID_Literal)\
                and not isinstance(iri_id.__dict__.get('type'), IRI_ID):
            warnings.warn("iri_id_num() accepts an IRI_ID as input.")
        super(id_to_iri, self).__init__(iri_id, **kw)


class iri_to_id(GenericFunction):
    """Get an IRI ID from an IRI.
    If the IRI is new to virtuoso, the IRI ID may be created on-the-fly,
    according to the second argument."""
    type = IRI_ID
    name = "iri_to_id"

    def __init__(self, iri, create=True, **kw):
        if isinstance(iri, past.builtins.unicode):
            iri = iri_to_uri(iri)
        if not isinstance(iri, str):
            warnings.warn("iri_id_num() accepts an IRI (VARCHAR) as input.")
        super(iri_to_id, self).__init__(iri, create, **kw)


def iri_property(iri_id_colname, iri_propname):
    """Class decorator to add access to an IRI_ID column as an IRI.
    The name of the IRI property will be iri_propname."""
    def iri_class_decorator(klass):
        iri_hpropname = '_'+iri_propname
        setattr(klass, iri_hpropname,
                column_property(id_to_iri(getattr(klass, iri_id_colname))))

        def iri_accessor(self):
            return getattr(self, iri_hpropname)

        def iri_expression(klass):
            return id_to_iri(getattr(klass, iri_id_colname))

        def iri_setter(self, val):
            setattr(self, iri_hpropname, val)
            setattr(self, iri_id_colname, iri_to_id(val))

        def iri_deleter(self):
            setattr(self, iri_id_colname, None)

        col = getattr(klass, iri_id_colname)
        if not col.property.columns[0].nullable:
            iri_deleter = None
        prop = hybrid_property(
            iri_accessor, iri_setter, iri_deleter, iri_expression)
        setattr(klass, iri_propname, prop)
        return klass
    return iri_class_decorator


class ANY(Text):
    __visit_name__ = 'ANY'

class XML(Text):
    __visit_name__ = 'XML'


class LONGXML(Text):
    __visit_name__ = 'LONG_XML'


class VirtuosoTypeCompiler(compiler.GenericTypeCompiler):
    def visit_boolean(self, type_):
        return self.visit_SMALLINT(type_)

    def visit_LONG_VARCHAR(self, type_):
        return 'LONG VARCHAR'

    def visit_LONG_NVARCHAR(self, type_):
        return 'LONG NVARCHAR'

    def visit_DOUBLE_PRECISION(self, type_):
        return 'DOUBLE PRECISION'

    def visit_BIGINT(self, type_):
        return "INTEGER"

    def visit_DATE(self, type_):
        return "CHAR(10)"

    def visit_CLOB(self, type_):
        return self.visit_LONG_VARCHAR(type_)

    def visit_NCLOB(self, type_):
        return self.visit_LONG_NVARCHAR(type_)

    def visit_TEXT(self, type_):
        return self._render_string_type(type_, "LONG VARCHAR")

    def visit_BLOB(self, type_):
        return "LONG VARBINARY"

    def visit_BINARY(self, type_):
        return self.visit_VARBINARY(type_)

    def visit_VARBINARY(self, type_):
        return "VARBINARY" + (type_.length and "(%d)" % type_.length or "")

    def visit_LONG_VARBINARY(self, type_):
        return 'LONG VARBINARY'

    def visit_large_binary(self, type_):
        return self.visit_LONG_VARBINARY(type_)

    def visit_unicode(self, type_):
        return self.visit_NVARCHAR(type_)

    def visit_text(self, type_):
        return self.visit_TEXT(type_)

    def visit_unicode_text(self, type_):
        return self.visit_LONG_NVARCHAR(type_)

    def visit_IRI_ID(self, type_):
        return "IRI_ID"

    def visit_XML(self, type_):
        return "XML"

    def visit_LONG_XML(self, type_):
        return "LONG XML"

    def visit_ANY(self, type_):
        return "VARCHAR(8192)"

    # def visit_user_defined(self, type_):
    # TODO!
    #     return type_.get_col_spec()





class AddForeignKey(_CreateDropBase):
    """Represent an ALTER TABLE ADD CONSTRAINT statement."""

    __visit_name__ = "add_foreign_key"

    def __init__(self, element, *args, **kw):
        super(AddForeignKey, self).__init__(element, *args, **kw)
        element._create_rule = util.portable_instancemethod(
            self._create_rule_disable)


class DropForeignKey(_CreateDropBase):
    """Represent an ALTER TABLE DROP CONSTRAINT statement."""

    __visit_name__ = "drop_foreign_key"

    def __init__(self, element, cascade=False, **kw):
        self.cascade = cascade
        super(DropForeignKey, self).__init__(element, **kw)
        element._create_rule = util.portable_instancemethod(
            self._create_rule_disable)


class VirtuosoDDLCompiler(compiler.DDLCompiler):
    def get_column_specification(self, column, **kwargs):
        colspec = (self.preparer.format_column(column) + " "
                   + self.dialect.type_compiler.process(column.type))

        if column.nullable is not None:
            if not column.nullable or column.primary_key or \
                    isinstance(column.default, schema.Sequence):
                colspec += " NOT NULL"
            else:
                colspec += " NULL"

        if column.table is None:
            raise exc.CompileError(
                "virtuoso requires Table-bound columns "
                "in order to generate DDL")

        # install an IDENTITY Sequence if we either a sequence
        # or an implicit IDENTITY column
        if isinstance(column.default, schema.Sequence):
            if column.default.start == 0:
                start = 0
            else:
                start = column.default.start or 1

            colspec += " IDENTITY (START WITH %s)" % (start,)
        elif column is column.table._autoincrement_column:
            colspec += " IDENTITY"
        else:
            default = self.get_column_default_string(column)
            if default is not None:
                colspec += " DEFAULT " + default

        return colspec

    def visit_under_constraint(self, constraint):
        table = constraint.table
        parent_table = constraint.parent_table
        return "UNDER %s.%s " % (
            self.preparer.quote_schema(
                parent_table.schema, table.quote_schema),
            self.preparer.quote(parent_table.name, table.quote))

    def visit_drop_foreign_key(self, drop):
        # Make sure the constraint has no name, ondelete, deferrable, onupdate
        constraint = drop.element.constraint
        names = ("name", "ondelete", "deferrable", "onupdate")
        temp = {name: getattr(constraint, name, None) for name in names}
        for name in names:
            setattr(constraint, name, None)
        result = "ALTER TABLE %s DROP %s" % (
            self.preparer.format_table(drop.element.parent.table),
            self.visit_foreign_key_constraint(constraint),
        )
        for name in names:
            setattr(constraint, name, temp[name])
        return result


    def visit_add_foreign_key(self, create):
        return "ALTER TABLE %s ADD %s" % (
            self.preparer.format_table(create.element.parent.table),
            self.visit_foreign_key_constraint(create.element.constraint),
        )

    def visit_create_text_index(self, create, include_schema=False,
                           include_table_schema=True):
        text_index = create.element
        column = text_index.column
        params = dict(table=column.table.name, column=column.name)
        for x in ('xml','clusters','key','with_insert','transform','language','encoding'):
            params[x] =''
        if isinstance(column.type, (XML, LONGXML)):
            params['xml'] = 'XML'
        else:
            assert isinstance(column.type, TEXT_TYPES)
        if text_index.clusters:
            params['clusters'] = 'CLUSTERED WITH (' + ','.join((
                self.preparer.quote(c.name) for c in text_index.clusters)) + ')'
        if text_index.key:
            params['key'] = 'WITH KEY ' + self.preparer.quote(text_index.key.name)
        if not text_index.do_insert:
            params['with_insert'] = 'NO INSERT'
        if text_index.transform:
            params['transform'] = 'USING ' + self.preparer.quote(text_index.transform)
        if text_index.language:
            params['language'] = "LANGUAGE '" + text_index.language + "'"
        if text_index.encoding:
            params['encoding'] = 'ENCODING ' + text_index.encoding
        return ('CREATE TEXT {xml} INDEX ON "{table}" ( "{column}" ) {key} '
                '{with_insert} {clusters} {transform} {language} {encoding}'
                ).format(**params)

    def visit_drop_text_index(self, drop):
        text_index = drop.element
        name = "{table}_{column}_WORDS".format(
            table=text_index.column.table.name,
            column=text_index.column.name)
        return '\nDROP TABLE %s.%s' % (
            self.preparer.quote_schema(text_index.table.schema),
            self.preparer.quote(name))

# TODO: Alter is weird. Use MODIFY with full new thing. Eg:
# ALTER TABLE assembl..imported_post MODIFY body_mime_type NVARCHAR NOT NULL

##??TODO fixme recheck types XML end etc
ischema_names = {
    'bigint': INTEGER,
    'int': INTEGER,
    'integer': INTEGER,
    'smallint': SMALLINT,
    'tinyint': SMALLINT,
    'unsigned bigint': INTEGER,
    'unsigned int': INTEGER,
    'unsigned smallint': SMALLINT,
    'numeric': NUMERIC,
    'decimal': DECIMAL,
    'dec': DECIMAL,
    'float': FLOAT,
    'double': DOUBLEPRECISION,
    'double precision': DOUBLEPRECISION,
    'real': REAL,
    'smalldatetime': DATETIME,
    'datetime': DATETIME,
    'date': CHAR,
    'time': TIME,
    'char': CHAR,
    'character': CHAR,
    'varchar': VARCHAR,
    'character varying': VARCHAR,
    'char varying': VARCHAR,
    'nchar': NCHAR,
    'national char': NCHAR,
    'national character': NCHAR,
    'nvarchar': NVARCHAR,
    'nchar varying': NVARCHAR,
    'national char varying': NVARCHAR,
    'national character varying': NVARCHAR,
    'text': LONGVARCHAR,
    'unitext': LONGNVARCHAR,
    'binary': VARBINARY,
    'varbinary': VARBINARY,
    'long varbinary': LONGVARBINARY,
    'long varchar': LONGVARCHAR,
    'timestamp': TIMESTAMP,
    'any': ANY,
}


# DO NOT USE! Deprecated in Columnar view.
class UnderConstraint(Constraint):
    __visit_name__ = 'under_constraint'

    def __init__(self, parent_table, **kw):
        super(UnderConstraint, self).__init__(**kw)
        if not isinstance(parent_table, Table)\
                and parent_table.__dict__.get('__table__') is not None:
            parent_table = parent_table.__table__
        assert isinstance(parent_table, Table)
        self.parent_table = parent_table


class VirtuosoDialect(PyODBCConnector, default.DefaultDialect):
    name = 'virtuoso'
    execution_ctx_cls = VirtuosoExecutionContext
    preparer = VirtuosoIdentifierPreparer
    statement_compiler = VirtuosoSQLCompiler
    type_compiler = VirtuosoTypeCompiler
    ischema_names = ischema_names
    supports_unicode_statements = True
    supports_unicode_binds = True
    returns_unicode_strings = True
    supports_native_boolean = False
    ddl_compiler = VirtuosoDDLCompiler
    supports_right_nested_joins = False
    supports_multivalues_insert = False
    supports_statement_cache = True
    supports_sequences = True
    postfetch_lastrowid = True
    supports_native_decimal = True
    schema_name = None

    def __init__(
        self,
        schema_name=None,
        **opts,
    ):
        self.schema_name = schema_name
        super().__init__(**opts)
     

    def connect(self, *args, **kwargs):
        connection = super(VirtuosoDialect, self).connect(*args, **kwargs)
        return connection

    def _get_default_schema_name(self, connection):
        res = connection.execute(
            text('select dbname(), get_user()'))
        catalog, schema = res.fetchone()
        self.default_cat = catalog
        if self.schema_name is not None:
            return self.schema_name
        else:
            return schema


    def _get_path(self, schema=None, **kw):
        if schema is None:
            schema = self.default_schema_name
        if schema is None:
            return (self.default_cat, None)
        if '.' not in schema:
            return (self.default_cat, schema)
        else:
            return schema.split('.', 1)


    def get_default_isolation_level(self, connection):
        return "READ COMMITTED"


    def has_schema(self, connection, schema, **kw):
        catalog, schema_ = self._get_path(schema)
        params = [catalog, schema];

        sql=("SELECT TABLE_NAME FROM DB..TABLES \n"
                 "WHERE upper(TABLE_CATALOG) like upper(?) \n"
                 "  AND upper(TABLE_SCHEMA) like upper(?)")

        row = connection.connection.execute(sql, tuple(params)).fetchone()
        if row:
           return True
        else:
           return False
         

    def has_table(self, connection, table_name, schema=None, **kw):
        catalog, schema_ = self._get_path(schema)
        params = [catalog, schema_, table_name];

        sql=("SELECT TABLE_NAME FROM DB..TABLES \n"
                 "WHERE upper(TABLE_CATALOG) like upper(?) \n"
                 "  AND upper(TABLE_SCHEMA) like upper(?) \n"
                 "  AND upper(TABLE_NAME) = upper(?) ")

        row = connection.connection.execute(sql, tuple(params)).fetchone()
        if row:
           return True
        else:
           return False


    def has_sequence(self, connection, sequence_name, schema=None):
        # sequences are auto-created in virtuoso
        return True



    def get_table_names(self, connection, schema=None, **kw):
        catalog, schema_ = self._get_path(schema)
        params = [catalog, schema_];

        sql=("SELECT TABLE_NAME FROM DB..TABLES \n"
                 "WHERE upper(TABLE_CATALOG) like upper(?) \n"
                 " AND upper(TABLE_SCHEMA) like upper(?) ")

        ret = []
        for row in connection.connection.execute(sql, tuple(params)):
            ret.append(row.TABLE_NAME)
        return ret


    @reflection.cache
    def get_schema_names(self, connection, **kw):
        catalog, schema = self._get_path(None)
        params = [catalog];

        sql=("select distinct \n" +
             " name_part(KEY_TABLE, 1) AS TABLE_SCHEM VARCHAR(128) \n" +
             "from DB.DBA.SYS_KEYS \n"
             "where upper(name_part(SYS_KEYS.KEY_TABLE,0)) like upper(?)")

        ret = []
        for row in connection.connection.execute(sql, tuple(params)):
           ret.append(row.TABLE_SCHEM)
        return ret

    
    def get_view_names(self, connection, schema=None, **kw):
        catalog, schema_ = self._get_path(schema)
        params = [catalog, schema_];

        sql=("SELECT TABLE_NAME FROM DB..VIEWS \n"
                 "WHERE upper(TABLE_CATALOG) like upper(?) \n"
                 " AND upper(TABLE_SCHEMA) like upper(?) ")

        ret = []
        for row in connection.connection.execute(sql, tuple(params)):
            ret.append(row.TABLE_NAME)
        return ret


    def get_view_definition(self, connection, view_name, schema=None, **kw):
        catalog, schema = self._get_path(schema)
        params = [catalog, schema_, view_name];

        sql=("SELECT VIEW_DEFINITION FROM DB..VIEWS \n"
                 "WHERE upper(TABLE_CATALOG) like upper(?) \n"
                 " AND upper(TABLE_SCHEMA) like upper(?) \n"
                 " AND upper(TABLE_NAME) = upper(?) ")

        ret = []
        row = connection.connection.execute(sql, tuple(params)).fetchone()
        if row:
           return row.VIEW_DEFINITION
        else:
           return ''


    @reflection.cache
    def get_columns(self, connection, table_name, schema=None, **kw):
        catalog, schema_ = self._get_path(schema)
        params = [catalog, schema_, table_name]

        sql=("select\n"
             " name_part (k.KEY_TABLE,0) AS TABLE_CAT VARCHAR(128),\n"
             " name_part (k.KEY_TABLE,1) AS TABLE_SCHEM VARCHAR(128),\n"
             " name_part (k.KEY_TABLE,2) AS TABLE_NAME VARCHAR(128), \n"
             " c.\"COLUMN\" AS COLUMN_NAME VARCHAR(128), \n"
             " dv_to_sql_type3(c.COL_DTP) AS DATA_TYPE SMALLINT,\n"
             " case when (c.COL_DTP in (125, 132) and get_keyword ('xml_col', coalesce (c.COL_OPTIONS, vector ())) is not null) then 'XMLType' else dv_type_title(c.COL_DTP) end AS TYPE_NAME VARCHAR(128),\n" 
             " c.COL_PREC AS COLUMN_SIZE INTEGER,\n"
             " c.COL_PREC AS BUFFER_LENGTH INTEGER,\n"
             " c.COL_SCALE AS DECIMAL_DIGITS SMALLINT,\n"
             " 2 AS NUM_PREC_RADIX SMALLINT,\n"
             " case c.COL_NULLABLE when 1 then 0 else 1 end AS NULLABLE SMALLINT,\n"
             " NULL AS REMARKS VARCHAR(254), \n"
             " deserialize (c.COL_DEFAULT) AS COLUMN_DEF VARCHAR(254), \n"
             " case 1 when 1 then dv_to_sql_type3(c.COL_DTP) else dv_to_sql_type(c.COL_DTP) end AS SQL_DATA_TYPE SMALLINT,\n"
             " case c.COL_DTP when 129 then 1 when 210 then 2 when 211 then 3 else NULL end AS SQL_DATETIME_SUB SMALLINT,\n"
             " c.COL_PREC AS CHAR_OCTET_LENGTH INTEGER,\n"
             " cast ((select count(*) from DB.DBA.SYS_COLS where \\TABLE = k.KEY_TABLE and COL_ID <= c.COL_ID) as INTEGER) AS ORDINAL_POSITION INTEGER, \n"
             " case c.COL_NULLABLE when 1 then 'NO' else 'YES' end AS IS_NULLABLE VARCHAR, \n"
             " c.COL_CHECK as COL_CHECK \n"
             "from DB.DBA.SYS_KEYS k, DB.DBA.SYS_KEY_PARTS kp, DB.DBA.SYS_COLS c \n"
             "where upper (name_part (k.KEY_TABLE,0)) like upper (?)\n"
             "  and upper (name_part (k.KEY_TABLE,1)) like upper (?)\n"
             "  and upper (name_part (k.KEY_TABLE,2)) = upper (?)\n"
             "  and c.\"COLUMN\" <> '_IDN' \n"
             "  and k.KEY_IS_MAIN = 1\n"
             "  and k.KEY_MIGRATE_TO is null\n"
             "  and kp.KP_KEY_ID = k.KEY_ID\n"
             "  and COL_ID = KP_COL\n"
             "order by KEY_TABLE, 17\n")

        ret = []
        for row in connection.connection.execute(sql, tuple(params)):
           class_ = ischema_names[row.TYPE_NAME.lower()]
           type_ = class_()
           if class_ is types.String:
               type_.length = row.COLUMN_SIZE
           elif class_ in [types.DECIMAL, types.NUMERIC]:
               type_.precision = row.COLUMN_SIZE
               type_.scale = row.DECIMAL_DIGITS

           ret.append(
               { "name": row.COLUMN_NAME,
                 "type": type_,
                 "nullable": bool(row.NULLABLE),
                 "default": row.COLUMN_DEF,
                 "autoincrement": row.COL_CHECK.find("I")!=-1,
               })
        return ret


    @reflection.cache
    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        catalog, schema_ = self._get_path(schema)
        params = [catalog, schema_, table_name]

        sql=("select"
             " name_part(v1.KEY_TABLE,0) AS \\TABLE_QUALIFIER VARCHAR(128), \n"
             " name_part(v1.KEY_TABLE,1) AS \\TABLE_OWNER VARCHAR(128), \n"
             " name_part(v1.KEY_TABLE,2) AS \\TABLE_NAME VARCHAR(128), \n"
             " DB.DBA.SYS_COLS.\\COLUMN AS \\COLUMN_NAME VARCHAR(128), \n"
             " (kp.KP_NTH+1) AS \\KEY_SEQ SMALLINT, \n"
             " name_part (v1.KEY_NAME, 2) AS \\PK_NAME VARCHAR(128), \n"
             " name_part(v2.KEY_TABLE,0) AS \\ROOT_QUALIFIER VARCHAR(128), \n"
             " name_part(v2.KEY_TABLE,1) AS \\ROOT_OWNER VARCHAR(128), \n"
             " name_part(v2.KEY_TABLE,2) AS \\ROOT_NAME VARCHAR(128) \n"
             "from DB.DBA.SYS_KEYS v1, DB.DBA.SYS_KEYS v2, \n"
             "     DB.DBA.SYS_KEY_PARTS kp, DB.DBA.SYS_COLS \n"
             "where upper(name_part(v1.KEY_TABLE,0)) like upper(?) \n"
             "  and upper(name_part(v1.KEY_TABLE,1)) like upper(?) \n"
             "  and upper(name_part(v1.KEY_TABLE,2)) = upper(?) \n"
             "  and v1.KEY_IS_MAIN = 1 \n"
             "  and v1.KEY_MIGRATE_TO is NULL \n"
             "  and v1.KEY_SUPER_ID = v2.KEY_ID \n"
             "  and kp.KP_KEY_ID = v1.KEY_ID \n"
             "  and kp.KP_NTH < v1.KEY_DECL_PARTS \n"
             "  and DB.DBA.SYS_COLS.COL_ID = kp.KP_COL \n"
             "  and DB.DBA.SYS_COLS.\\COLUMN <> '_IDN' \n"
             "order by v1.KEY_TABLE, kp.KP_NTH")

        data = connection.connection.execute(sql, tuple(params)).fetchall();

        ret = None
        if len(data) > 0:
            ret = { "constrained_columns": [row.COLUMN_NAME for row in data],
                    "name": data[0].PK_NAME,
                  }
        return ret


    @reflection.cache
    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        catalog, schema_ = self._get_path(schema)
        params = [catalog, schema_, table_name]

        sql=("select"
             " name_part (PK_TABLE, 0) as PKTABLE_QUALIFIER varchar (128),"
             " name_part (PK_TABLE, 1) as PKTABLE_OWNER varchar (128),"
             " name_part (PK_TABLE, 2) as PKTABLE_NAME varchar (128),"
             " PKCOLUMN_NAME as PKCOLUMN_NAME varchar (128),"
             " name_part (FK_TABLE, 0) as FKTABLE_QUALIFIER varchar (128),"
             " name_part (FK_TABLE, 1) as FKTABLE_OWNER varchar (128),"
             " name_part (FK_TABLE, 2) as FKTABLE_NAME varchar (128),"
             " FKCOLUMN_NAME as FKCOLUMN_NAME varchar (128),"
             " (KEY_SEQ + 1) as KEY_SEQ SMALLINT,"
             " FK_NAME as FK_NAME varchar (128),"
             " PK_NAME as PK_NAME varchar (128) "
             "from DB.DBA.SYS_FOREIGN_KEYS "
             "where upper (name_part (FK_TABLE, 0)) like upper (?) \n"
             "  and upper (name_part (FK_TABLE, 1)) like upper (?) \n"
             "  and upper (name_part (FK_TABLE, 2)) = upper (?) \n"
             "order by 1, 2, 3, 5, 6, 7, 9")

        def fkey_rec():
            return {
                "name": None,
                "constrained_columns": [],
                "referred_schema": None,
                "referred_table": None,
                "referred_columns": [],
                "options": {},
            }

        fkeys = defaultdict(fkey_rec)

        crs = connection.connection.execute(sql, tuple(params))
        for row in crs:
          rec = fkeys[row.FK_NAME]
          rec["name"] = row.FK_NAME

          c_cols = rec["constrained_columns"]
          c_cols.append(row.FKCOLUMN_NAME)

          r_cols = rec["referred_columns"]
          r_cols.append(row.PKCOLUMN_NAME)

          if not rec["referred_table"]:
            rec["referred_table"] = row.PKTABLE_NAME
            rec["referred_schema"] = schema  ##row.PKTABLE_QUALIFIER+"."+row.PKTABLE_OWNER
  
        return list(fkeys.values())


    @reflection.cache
    def get_indexes(self, connection, table_name, schema=None, **kw):
        catalog, schema_ = self._get_path(schema)
        params = [catalog, schema_, table_name, catalog, schema_, table_name];

        sql=("select\n"
             " name_part(SYS_KEYS.KEY_TABLE,0) AS \\TABLE_QUALIFIER VARCHAR(128),\n"
             " name_part(SYS_KEYS.KEY_TABLE,1) AS \\TABLE_OWNER VARCHAR(128),\n"
             " name_part(SYS_KEYS.KEY_TABLE,2) AS \\TABLE_NAME VARCHAR(128),\n"
             " iszero(SYS_KEYS.KEY_IS_UNIQUE) AS \\NON_UNIQUE SMALLINT,\n"
             " name_part (SYS_KEYS.KEY_TABLE, 0) AS \\INDEX_QUALIFIER VARCHAR(128),\n"
             " name_part (SYS_KEYS.KEY_NAME, 2) AS \\INDEX_NAME VARCHAR(128),\n"
             " (SYS_KEY_PARTS.KP_NTH+1) AS \\SEQ_IN_INDEX SMALLINT,\n"
             " SYS_COLS.\\COLUMN AS \\COLUMN_NAME VARCHAR(128)\n"
             "from DB.DBA.SYS_KEYS SYS_KEYS, DB.DBA.SYS_KEY_PARTS SYS_KEY_PARTS,\n"
             " DB.DBA.SYS_COLS SYS_COLS \n"
             "where upper(name_part(SYS_KEYS.KEY_TABLE,0)) like upper(?)\n"
             "  and upper(name_part(SYS_KEYS.KEY_TABLE,1)) like upper(?)\n"
             "  and upper(name_part(SYS_KEYS.KEY_TABLE,2)) = upper(?)\n"
             "  and SYS_KEYS.KEY_IS_UNIQUE >= 0\n"
             "  and SYS_KEYS.KEY_MIGRATE_TO is NULL\n"
             "  and SYS_KEY_PARTS.KP_KEY_ID = SYS_KEYS.KEY_ID\n"
             "  and SYS_KEY_PARTS.KP_NTH < SYS_KEYS.KEY_DECL_PARTS\n"
             "  and SYS_COLS.COL_ID = SYS_KEY_PARTS.KP_COL\n"
             "  and SYS_COLS.\\COLUMN <> '_IDN' \n"
             "union all \n"
             "select\n"
             " name_part(SYS_KEYS.KEY_TABLE,0) AS \\TABLE_QUALIFIER VARCHAR(128),\n"
             " name_part(SYS_KEYS.KEY_TABLE,1) AS \\TABLE_OWNER VARCHAR(128),\n"
             " name_part(SYS_KEYS.KEY_TABLE,2) AS \\TABLE_NAME VARCHAR(128),\n"
             " NULL AS \\NON_UNIQUE SMALLINT,\n"
             " NULL AS \\INDEX_QUALIFIER VARCHAR(128),\n"
             " NULL AS \\INDEX_NAME VARCHAR(128),\n"
             " NULL AS \\SEQ_IN_INDEX SMALLINT,\n"
             " NULL AS \\COLUMN_NAME VARCHAR(128)\n"
             "from DB.DBA.SYS_KEYS SYS_KEYS\n"
             "where upper(name_part(SYS_KEYS.KEY_TABLE,0)) like upper(?)\n"
             "  and upper(name_part(SYS_KEYS.KEY_TABLE,1)) like upper(?)\n"
             "  and upper(name_part(SYS_KEYS.KEY_TABLE,2)) = upper(?)\n"
             "  and SYS_KEYS.KEY_IS_MAIN = 1\n"
             "  and SYS_KEYS.KEY_MIGRATE_TO is NULL\n"
             "order by 1,2,3,6,7")

        ret = {}
        for row in connection.connection.execute(sql, tuple(params)).fetchall():
            if row.INDEX_NAME is not None:
                if row.INDEX_NAME in ret:
                    ret[row.INDEX_NAME]["column_names"].append(
                        row.COLUMN_NAME
                    )
                else:
                    ret[row.INDEX_NAME] = {
                        "name": row.INDEX_NAME,
                        "unique": row.NON_UNIQUE == 0,
                        "column_names": [row.COLUMN_NAME],
                    }
        return [x[1] for x in ret.items()]


