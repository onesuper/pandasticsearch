### 0.4.0

* support filter function against ES version >= 5.0
* support customized terms aggregation: `df.groupby(df.age.terms(limit=10, include=[1, 2, 3]))`
* support aggregation metric alias

### 0.3.0

* support groupby date interval: `df.groupby(df.date.date_interval('1d'))`
* parameter change: `DataFrame.from_es(..., index=...)` to `DataFrame.from_es(url=..., index=...)`

### 0.2.0

* support metric agg: `stats`, `extended_stats`
* support boolean filter: `like`, `rlike`, `startswith`, `notnull`
* display time in `df.show()`

### 0.1.0

* support groupby ranges: `df.groupby(df.age.ranges([10,12,14]))`
* support script filter : `df.filter(ScriptFilter('2016 - doc["age"].value > 1995'))`
* support script sort : `df.sort(ScriptSort('doc["age"].value * 2'))`