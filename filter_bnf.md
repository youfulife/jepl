
文法描述

```
<expression>           ::= <term> { OR <term> }
<term>                 ::= <factor> { AND <factor> }
<factor>               ::= <predicate> | '(' <expression> ')'
<predicate>            ::= <comp_predicate> | <in_predicate>

<comp_predicate>       ::= string_lit <comp_op> (num_lit | regex_lit | string_lit)
<comp_op>              ::= '>' | '<' | '=' | '>=' | '<=' | '!=' | '<>' | '=~' | '!~'

<in_predicate>         ::= string_lit (NI|IN) <list>
<list>                 ::= '[' <elements> ']'
<elements>             ::= <element> {',' <element>}
<element>              ::= num_lit | string_lit

```
