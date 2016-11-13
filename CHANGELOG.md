### 0.1.3

* support script filter : `df.filter(df.statisfy_script('2016 - doc["age"].value > 1995'))`
* support script sort : `df.sort(ScriptSort('doc["age"].value * 2'))`


### 0.1.2

* support groupby ranges: `df.groupby(df.age.ranges([10,12,14]))`
* fix bug: query result not rendered as agg when groupby is empty