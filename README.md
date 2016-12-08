# Introduction
JSON Event Process Language is a SQL-like query language providing features specific to process json event and generate time series data.

The source code is based on [Influxql](https://github.com/influxdata/influxdb/tree/master/influxql).

The source code is More concise and focused.

# Notation
The syntax is specified using Extended Backus-Naur Form ("EBNF").

Notation operators in order of increasing precedence:

```
|   alternation
()  grouping
[]  option (0 or 1 times)
{}  repetition (0 to n times)
```

## Query representation

### Characters

Jepl is Unicode text encoded in [UTF-8](http://en.wikipedia.org/wiki/UTF-8).

```
newline             = /* the Unicode code point U+000A */ .
unicode_char        = /* an arbitrary Unicode code point except newline */ .
```

## Letters and digits

Letters are the set of ASCII characters plus the underscore character _ (U+005F)
is considered a letter.

Only decimal digits are supported.

```
letter              = ascii_letter | "_" .
ascii_letter        = "A" … "Z" | "a" … "z" .
digit               = "0" … "9" .
```

## Identifiers

Identifiers are tokens which refer to topic names, field keys.

The rules:

- must start with an upper or lowercase ASCII character or "_"
- may contain only ASCII letters, decimal digits, and "_"

```
identifier          = ( letter ) { letter | digit }
```

#### Examples:

```
cpu
_cpu_stats
```

## Keywords

```
ALL           AS            NI         IN
SELECT        WHERE         FROM       AND
OR
```

## Literals

### Integers

Jepl supports decimal integer literals.  Hexadecimal and octal literals are not currently supported.

```
int_lit             = ( "1" … "9" ) { digit }
```

### Floats

Jepl supports floating-point literals.  Exponents are not currently supported.

```
float_lit           = int_lit "." int_lit
```

### Strings

String literals must be surrounded by single quotes or double quotes. Strings may contain `'` or `"`
characters as long as they are escaped (i.e., `\'`, `\"`).

```
string_lit          = (`'` { unicode_char } `'`) | (`"` { unicode_char } `"`)
```

### Booleans

```
bool_lit            = TRUE | FALSE
```

### Regular Expressions

```
regex_lit           = "/" { unicode_char } "/"
```

**Comparators:**
`=~` matches against
`!~` doesn't match against

## Statement

```
statement        = select_stmt
```
### SELECT

```
select_stmt      = "SELECT" fields [from_clause] [ where_clause ]
```

### Fields

```
fields           = field { "," field }

field            = metric_expr [ alias ]

alias            = "AS" identifier

metric_expr      = metric_term { "+" | "-"  metric_term }

metric_term      = metric_factor { "*" | "/" metric_factor }

metric_factor    =  int_lit | float_lit | func "(" arg_expr ")"

func             = "SUM" | "COUNT" | "MAX" | "MIN" | "AVG"

```

### Metric Argument Expression

```
arg_expr         =  arg_term { "+" | "-"  arg_term }

arg_term         = arg_factor { "*" | "/" arg_factor }

arg_factor       = int_lit | float_lit | var_ref | "(" arg_expr ")"
```

### Clauses

```
from_clause      = "FROM" identifier

where_clause     = "WHERE" cond_expr
```

### Where Condition Expression
```
cond_expr        = unary_expr { binary_op unary_expr }

unary_expr       = "(" cond_expr ")" | var_ref | literal | list

binary_op        = "+" | "-" | "*" | "/" | "AND" | "OR" | "=" | "!=" | "<>" | "<" | "<=" | ">" | ">=" | "!~" | "=~" | "NI" | "IN"

var_ref          = identifier { "." identifier}

list             = "[" literal { "," literal } "]"

literal          = string_lit | int_lit | float_lit | bool_lit | regex_lit

```

#### Examples:

```sql

SELECT sum(tcp.bytes_in+tcp.bytes_out) AS total_bytes FROM packetbeat WHERE uid = 1 AND tcp.ip = '127.0.0.1'
```
