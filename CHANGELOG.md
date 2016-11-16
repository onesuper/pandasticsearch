
### 0.2.0

* support metric agg: `stats`, `extended_stats`
* support boolean filter: `like`, `rlike`, `startswith`, `notnull`
* display time in `df.show()`

### 0.1.0

* support groupby ranges: `df.groupby(df.age.ranges([10,12,14]))`
* support script filter : `df.filter(ScriptFilter('2016 - doc["age"].value > 1995'))`
* support script sort : `df.sort(ScriptSort('doc["age"].value * 2'))`