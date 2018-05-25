# select_parser.py
# Copyright 2010, Paul McGuire
#
# a simple SELECT statement parser, taken from SQLite's SELECT statement
# definition at http://www.sqlite.org/lang_select.html
#
from pyparsing import *

LPAR, RPAR, COMMA = map(Suppress, "(),")
select_stmt = Forward().setName("select statement")

# keywords
(UNION, ALL, AND, INTERSECT, EXCEPT, COLLATE, ASC, DESC, ON, USING, NATURAL, INNER,
 CROSS, LEFT, OUTER, JOIN, AS, INDEXED, NOT, SELECT, DISTINCT, FROM, WHERE, GROUP, BY,
 HAVING, ORDER, BY, LIMIT, OFFSET) = map(CaselessKeyword, """UNION, ALL, AND, INTERSECT, 
 EXCEPT, COLLATE, ASC, DESC, ON, USING, NATURAL, INNER, CROSS, LEFT, OUTER, JOIN, AS, INDEXED, NOT, SELECT, 
 DISTINCT, FROM, WHERE, GROUP, BY, HAVING, ORDER, BY, LIMIT, OFFSET""".replace(",", "").split())
(CAST, ISNULL, NOTNULL, NULL, IS, BETWEEN, ELSE, END, CASE, WHEN, THEN, EXISTS,
 COLLATE, IN, LIKE, GLOB, REGEXP, MATCH, ESCAPE, CURRENT_TIME, CURRENT_DATE,
 CURRENT_TIMESTAMP) = map(CaselessKeyword, """CAST, ISNULL, NOTNULL, NULL, IS, BETWEEN, ELSE, 
 END, CASE, WHEN, THEN, EXISTS, COLLATE, IN, LIKE, GLOB, REGEXP, MATCH, ESCAPE, 
 CURRENT_TIME, CURRENT_DATE, CURRENT_TIMESTAMP""".replace(",", "").split())
keyword = MatchFirst((UNION, ALL, INTERSECT, EXCEPT, COLLATE, ASC, DESC, ON, USING, NATURAL, INNER,
                      CROSS, LEFT, OUTER, JOIN, AS, INDEXED, NOT, SELECT, DISTINCT, FROM, WHERE, GROUP, BY,
                      HAVING, ORDER, BY, LIMIT, OFFSET, CAST, ISNULL, NOTNULL, NULL, IS, BETWEEN, ELSE, END, CASE, WHEN,
                      THEN, EXISTS,
                      COLLATE, IN, LIKE, GLOB, REGEXP, MATCH, ESCAPE, CURRENT_TIME, CURRENT_DATE,
                      CURRENT_TIMESTAMP))

identifier = ~keyword + Word(alphas, alphanums + "_")
collation_name = identifier.copy()
column_name = identifier.copy()
column_alias = identifier.copy()
table_name = identifier.copy()
table_alias = identifier.copy()
index_name = identifier.copy()
function_name = identifier.copy()
parameter_name = identifier.copy()
database_name = identifier.copy()

# expression
expr = Forward().setName("expression")

integer = Regex(r"[+-]?\d+")
numeric_literal = Regex(r"\d+(\.\d*)?([eE][+-]?\d+)?")
string_literal = QuotedString("'")
blob_literal = Combine(oneOf("x X") + "'" + Word(hexnums) + "'")
literal_value = (numeric_literal | string_literal | blob_literal |
                 NULL | CURRENT_TIME | CURRENT_DATE | CURRENT_TIMESTAMP)
bind_parameter = (
        Word("?", nums) |
        Combine(oneOf(": @ $") + parameter_name)
)
type_name = oneOf("TEXT REAL INTEGER BLOB NULL")

expr_term = (
        CAST + LPAR + expr + AS + type_name + RPAR |
        EXISTS + LPAR + select_stmt + RPAR |
        function_name + LPAR + Optional(delimitedList(expr)) + RPAR |
        literal_value |
        bind_parameter |
        identifier
)

UNARY, BINARY, TERNARY = 1, 2, 3
expr << operatorPrecedence(expr_term,
                           [
                               (oneOf('- + ~') | NOT, UNARY, opAssoc.LEFT),
                               ('||', BINARY, opAssoc.LEFT),
                               (oneOf('* / %'), BINARY, opAssoc.LEFT),
                               (oneOf('+ -'), BINARY, opAssoc.LEFT),
                               (oneOf('<< >> & |'), BINARY, opAssoc.LEFT),
                               (oneOf('< <= > >='), BINARY, opAssoc.LEFT),
                               (oneOf('= == != <>') | IS | IN | LIKE | GLOB | MATCH | REGEXP, BINARY, opAssoc.LEFT),
                               ('||', BINARY, opAssoc.LEFT),
                               ((BETWEEN, AND), TERNARY, opAssoc.LEFT),
                           ])

compound_operator = (UNION + Optional(ALL) | INTERSECT | EXCEPT)

ordering_term = expr + Optional(COLLATE + collation_name) + Optional(ASC | DESC)

join_constraint = Optional(ON + expr | USING + LPAR + Group(delimitedList(column_name)) + RPAR)

join_op = COMMA | (Optional(NATURAL) + Optional(INNER | CROSS | LEFT + OUTER | LEFT | OUTER) + JOIN)

join_source = Forward()
single_source = ((Group(database_name("database") + "." + table_name("table")) | table_name("table")) +
                 Optional(Optional(AS) + table_alias("table_alias")) +
                 Optional(INDEXED + BY + index_name("name") | NOT + INDEXED)("index") |
                 (LPAR + select_stmt + RPAR + Optional(Optional(AS) + table_alias)) |
                 (LPAR + join_source + RPAR))

join_source << single_source + ZeroOrMore(join_op + single_source + join_constraint)

result_column = "*" | table_name + "." + "*" | (expr + Optional(Optional(AS) + column_alias))
select_core = (SELECT + Optional(DISTINCT | ALL) + Group(delimitedList(result_column))("columns") +
               Optional(FROM + join_source) +
               Optional(WHERE + expr("where_expr")) +
               Optional(GROUP + BY + Group(delimitedList(ordering_term)("group_by_terms")) +
                        Optional(HAVING + expr("having_expr"))))

select_stmt << (select_core + ZeroOrMore(compound_operator + select_core) +
                Optional(ORDER + BY + Group(delimitedList(ordering_term))("order_by_terms")) +
                Optional(LIMIT + (integer + OFFSET + integer | integer + COMMA + integer)))

tests = """\
    SELECT click.mechanic_id,\n        convert_timezone('PST8PDT', click.ts) as click_ts,\n        click.clicked_type, \n        convert_timezone('PST8PDT',ac.ts) as add_ts,\n        ac.added_type,\n        ac.value,\n        m.name\nFROM\n(SELECT mechanic_id, ts, \nregexp_replace( regexp_substr(type, 'clicked_[^_]*'), 'clicked_') as clicked_type\nFROM posted_activities\nWHERE type in ('mechanic:clicked_timeslot_make_more_money',\n                'mechanic:clicked_zipcode_make_more_money',\n                'mechanic:clicked_tool_make_more_money')\n) click\nJOIN dw_mechanics m\nON m.id = click.mechanic_id\nLEFT JOIN\n(SELECT mechanic_id, ts, value,\nregexp_replace( regexp_substr(text, 'Mechanic added a [\\S]*'), 'Mechanic added a ') as added_type\nFROM activities\nWHERE event_type = 'mechanic:add_capability'\n) ac\nON click.mechanic_id = ac.mechanic_id\nAND click.clicked_type = ac.added_type\nAND click.ts <<= ac.ts""".splitlines()
for t in tests:
    print (t)
    try:
        print (select_stmt.parseString(t).dump())
    except ParseException as pe:
        print (pe.msg)
    print ()