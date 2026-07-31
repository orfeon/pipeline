---
type: Common
title: Expression
description: The numeric expression syntax shared by filter conditions and select/aggregation expression functions.
tags: [common, expression, formula]
timestamp: 2026-07-31T00:00:00Z
---

# Expression

This function evaluates data by assigning them to an expression given by a formula.
In a formula, you can specify field names for the data, and the values will be assigned at runtime.
All field values are converted to `double` type according to the schema of the original data.
Expressions use the [Lucene expressions](https://lucene.apache.org/core/10_5_0/expressions/org/apache/lucene/expressions/js/package-summary.html) JavaScript-like syntax and are compiled to JVM bytecode.
The following built-in operators and functions are available for use in formulas.

## Built-in operators

### Numeric operators

| function | description    |
|----------|----------------|
| `+`      | Addition       |
| `-`      | Subtraction    |
| `*`      | Multiplication |
| `/`      | Division       |
| `%`      | Modulo         |

Note: `^` is bitwise XOR, not exponentiation. Use `pow(base, exponent)` for powers.

### Comparison and logical operators

Comparison results are `1.0` for true and `0.0` for false. Logical operators treat non-zero values as true.

| function | description            |
|----------|------------------------|
| `==`     | Equals                 |
| `!=`     | Not equals             |
| `>`      | Greater than           |
| `>=`     | Greater than or equals |
| `<`      | Less than              |
| `<=`     | Less than or equals    |
| `!`      | NOT                    |
| `&&`     | AND                    |
| `&#124;&#124;` | OR               |

### Conditional (ternary) operator

`condition ? valueIfTrue : valueIfFalse`

Example: `amount > 1000 ? amount * 0.9 : amount`

### Bitwise operators

Operands are converted to 64-bit integers: `&#124;`, `&`, `^`, `~`, `<<`, `>>`, `>>>`

## Built-in functions

### Single argument functions

| function         | description                              |
|------------------|------------------------------------------|
| abs(`value`)     | absolute value                           |
| acos(`radian`)   | arc cosine                               |
| acosh(`value`)   | inverse hyperbolic cosine                |
| asin(`radian`)   | arc sine                                 |
| asinh(`value`)   | inverse hyperbolic sine                  |
| atan(`radian`)   | arc tangent                              |
| atanh(`value`)   | inverse hyperbolic tangent               |
| cbrt(`value`)    | cubic root                               |
| ceil(`value`)    | nearest upper integer                    |
| cos(`radian`)    | cosine                                   |
| cosh(`radian`)   | hyperbolic cosine                        |
| exp(`value`)     | euler's number raised to the power (e^x) |
| floor(`value`)   | nearest lower integer                    |
| ln(`value`)      | logarithmus naturalis (base e)           |
| log(`value`)     | logarithmus naturalis (base e)           |
| log10(`value`)   | logarithm (base 10)                      |
| log2(`value`)    | logarithm (base 2)                       |
| sin(`radian`)    | sine                                     |
| sinh(`radian`)   | hyperbolic sine                          |
| sqrt(`value`)    | square root                              |
| tan(`radian`)    | tangent                                  |
| tanh(`radian`)   | hyperbolic tangent                       |
| signum(`value`)  | signum function                          |

### Multi arguments functions

| function                                                    | description                                                                                          |
|-------------------------------------------------------------|------------------------------------------------------------------------------------------------------|
| pow(`base`, `exponent`)                                     | Returns `base` raised to the power of `exponent`.                                                    |
| logn(`base`, `value`)                                       | Returns the logarithm of `value` with the given `base`.                                              |
| atan2(`y`, `x`)                                             | Returns the angle theta from the conversion of rectangular coordinates to polar coordinates.         |
| if(`expr`, `true_result`, `else_result`)                    | If `expr` evaluates to `true`, returns `true_result`, else returns the evaluation for `else_result`. |
| switchN(`cond1`, `value1`, ..., `condN`, `valueN`)          | For N = 3..8 (e.g. `switch3`). Returns the value paired with the first true condition, or `0`.       |
| max(`value1`, `value2`)                                     | Returns the greater of two arguments values.                                                         |
| min(`value1`, `value2`)                                     | Returns the smaller of two arguments values.                                                         |
| timestamp_diff_millisecond(`timestamp1`, `timestamp2`)      | Returns the difference between two timestamps in milliseconds                                        |
| timestamp_diff_second(`timestamp1`, `timestamp2`)           | Returns the difference between two timestamps in seconds                                             |
| timestamp_diff_minute(`timestamp1`, `timestamp2`)           | Returns the difference between two timestamps in minutes                                             |
| timestamp_diff_hour(`timestamp1`, `timestamp2`)             | Returns the difference between two timestamps in hours                                               |
| timestamp_diff_day(`timestamp1`, `timestamp2`)              | Returns the difference between two timestamps in days                                                |
| timestamp_to_date(`timestamp_value`, `offset_hour`)         | Converts a timestamp value to a date with the specified offset hour                                  |

Timestamp function arguments are unix epoch microseconds.

## Data type mapping

With expression, data fields types are converted to `double` types with the following mapping.

| original data type  | description                                                                        |
|---------------------|------------------------------------------------------------------------------------|
| int16, int32, int64 | integer value as double value                                                      |
| float32, float64    | double value itself                                                                |
| numeric             | bigdecimal value as double value                                                   |
| boolean             | `1.0` if the value is true, `0.0` if false                                         |
| string              | Parse string value as double type                                                  |
| date                | double value of unix epoch days (days from `1970-01-01`)                           |
| timestamp           | double value of unix epoch microseconds (microseconds from `1970-01-01T00:00:00Z`) |

## Numerical constants

The following constants can be used in expressions

* `pi`: the value of π as defined in Math.PI
* `e`: the value of Euler's number e
