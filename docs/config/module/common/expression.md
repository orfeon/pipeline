# Expression

This page has been migrated.
See the canonical document at [src/main/resources/server/docs/module/common/expression.md](../../../../src/main/resources/server/docs/module/common/expression.md).

Note: the expression engine has been replaced with
[Lucene expressions](https://lucene.apache.org/core/10_5_0/expressions/org/apache/lucene/expressions/js/package-summary.html)
(JavaScript-like syntax). Notable syntax changes from the previous engine (exp4j):

- Equality is `==` (was `=`)
- Logical AND / OR are `&&` / `&#124;&#124;` (were `&` / `&#124;`)
- `^` is now bitwise XOR — use `pow(base, exponent)` for exponentiation
- The ternary operator `condition ? a : b` is available
