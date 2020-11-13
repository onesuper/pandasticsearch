## Pandasticsearch

[![Build Status](https://travis-ci.org/onesuper/pandasticsearch.svg?branch=master)](https://travis-ci.org/onesuper/pandasticsearch) [![PyPI](https://img.shields.io/pypi/v/pandasticsearch.svg)](https://pypi.python.org/pypi/pandasticsearch)


Pandasticsearch is an Elasticsearch client for data-analysis purpose.
It provides table-like access to Elasticsearch documents, similar
to the Python Pandas library and R DataFrames.

To install:

```
pip install pandasticsearch
# if you intent to export Pandas DataFrame 
pip install pandasticsearch[pandas]
```

  Elasticsearch is skilled in real-time indexing, search and data-analysis.
  Pandasticsearch can convert the analysis results (e.g. multi-level nested aggregation)
  into [Pandas](http://pandas.pydata.org) DataFrame objects for subsequent data analysis.
  

Checkout the API doc: [http://pandasticsearch.readthedocs.io/en/latest/](http://pandasticsearch.readthedocs.io/en/latest/).

## Usage

### DataFrame API

A `DataFrame` object accesses Elasticsearch data with high level operations.
It is type-safe, easy-to-use and Pandas-flavored.

```python
# Create a DataFrame object
from pandasticsearch import DataFrame
df = DataFrame.from_es(url='http://localhost:9200', index='people', username='abc', password='abc')

# Print the schema(mapping) of the index
df.print_schema()
# company
# |-- employee
#   |-- name: {'index': 'not_analyzed', 'type': 'string'}
#   |-- age: {'type': 'integer'}
#   |-- gender: {'index': 'not_analyzed', 'type': 'string'}

# Inspect the columns
df.columns
#['name', 'age', 'gender']

# Denote a column
df.name
# Column('name')
df['age']
# Column('age')

# Projection
df.filter(df.age < 25).select('name', 'age').collect()
# [Row(age=12,name='Alice'), Row(age=11,name='Bob'), Row(age=13,name='Leo')]

# Print the rows into console
df.filter(df.age < 25).select('name').show(3)
# +------+
# | name |
# +------+
# | Alice|
# | Bob  |
# | Leo  |
# +------+

# Convert to Pandas object for subsequent analysis
df[df.gender == 'male'].agg(df.age.avg).to_pandas()
#    avg(age)
# 0        12


# Dump all your dataset to Pandas DataFrame in memory for subsequent analysis
df.to_pandas()
# ...

# Limit your data amount, if your dataset is too large
df.limit(1000).to_pandas()
# ...


# Translate the DataFrame to an ES query (dictionary)
df[df.gender == 'male'].agg(df.age.avg).to_dict()
# {'query': {'filtered': {'filter': {'term': {'gender': 'male'}}}}, 'aggregations': {'avg(birthYear)':
# {'avg': {'field': 'birthYear'}}}, 'size': 0}

```



### Filter

```python
# Filter by a boolean condition
df.filter(df.age < 13).collect()
# [Row(age=12,gender='female',name='Alice'), Row(age=11,gender='male',name='Bob')]

# Filter by a set of boolean conditions (by &)
df.filter((df.age < 13) & (df.gender == 'male')).collect()
# Row(age=11,gender='male',name='Bob')]

# Filter by a set of boolean conditions (by chaining)
df.filter(df.age < 13).filter(df.gender == 'male').collect()
# Row(age=11,gender='male',name='Bob')]

# Filter by a wildcard (sql `like`)
df.filter(df.name.like('A*')).collect()
# [Row(age=12,gender='female',name='Alice')]

# Filter by a regular expression (sql `rlike`)
df.filter(df.name.rlike('A.l.e')).collect()
# [Row(age=12,gender='female',name='Alice')]

# Filter by a prefixed string pattern
df.filter(df.name.startswith('Al')).collect()
# [Row(age=12,gender='female',name='Alice')]

# Filter by a script
df.filter('2016 - doc["age"].value > 1995').collect()
# [Row(age=12,name='Alice'), Row(age=13,name='Leo')]
```


### Aggregation
```python
# Aggregation
df[df.gender == 'male'].agg(df.age.avg).collect()
# [Row(avg(age)=12)]

# Metric alias
df[df.gender == 'male'].agg(df.age.avg.alias('avg_age')).collect()
# [Row(avg_age=12)]

# Groupby only (will give the `doc_count`)
df.groupby('gender').collect()
# [Row(doc_count=1), Row(doc_count=2)]

# Groupby and then aggregate metric
df.groupby('gender').agg(df.age.max).collect()
# [Row(doc_count=1, max(age)=12), Row(doc_count=2, max(age)=13)]

# Groupby and then aggregate multiple metrics(max and value_count)
df.groupby('gender').agg(df.age.value_count, df.age.max,).collect()
# [Row(value_count(age)=1, max(age)=12), Row(value_count(age)=2, max(age)=13)]

# Group by a set of ranges
df.groupby(df.age.ranges([10,12,14])).to_pandas()
#                   doc_count
# range(10,12,14)
# 10.0-12.0                 2
# 12.0-14.0                 1

# Advanced ES aggregation
df.groupby(df.gender).agg(df.age.stats).to_pandas()
df.agg(df.age.extended_stats).to_pandas()
df.agg(df.age.percentiles).to_pandas()
df.groupby(df.date.date_interval('1d')).to_pandas()

# Customized aggregation terms
df.groupby(df.age.terms(size=5, include=[1, 2, 3]))
```

### Sort
```python
# Sort
df.sort(df.age.asc).select('name', 'age').collect()
# [Row(age=11,name='Bob'), Row(age=12,name='Alice'), Row(age=13,name='Leo')]

# Sort by a script
df.sort('doc["age"].value * 2').collect()
# [Row(age=11,name='Bob'), Row(age=12,name='Alice'), Row(age=13,name='Leo')]
```

## Use with Another Python Client

Pandasticsearch can also be used with another full featured Python client:

* [elasticsearch-py](https://github.com/elastic/elasticsearch-py) (Official)
* [Elasticsearch-SQL](https://github.com/NLPchina/elasticsearch-sql)
* [pyelasticsearch](https://github.com/pyelasticsearch/pyelasticsearch)
* [pyes](https://github.com/aparo/pyes)


### Build query

```Python
from pandasticsearch import DataFrame
body = df[df['gender'] == 'male'].agg(df['age'].avg).to_dict()
 
from elasticsearch import Elasticsearch
result_dict = es.search(index="recruit", body=body)
```

### Parse result

```python
from elasticsearch import Elasticsearch
es = Elasticsearch('http://localhost:9200')
result_dict = es.search(index="recruit", body={"query": {"match_all": {}}})

from pandasticsearch import Select
pandas_df = Select.from_dict(result_dict).to_pandas()
```


## Compatibility

An integer argument `compat`  needs to be passed to `from_es` to resolve compatibility issues (default 2):

### 5.0

```
df = DataFrame.from_es(url='http://localhost:9200', index='people', doc_type='mapping_name', compat=5)
```

For ES version under 7.0, a `doc_type` must be given to specify index mappings (it is deprecated in 7.0).


### 7.0


```
df = DataFrame.from_es(url='http://localhost:9200', index='people', compat=7)
```


## Related Articles

* [Spark and Elasticsearch for real-time data analysis](https://web.archive.org/web/20150911151523/https://spark-summit.org/2015-east/wp-content/uploads/2015/03/SSE15-35-Leau.pdf)


## LICENSE
 
MIT
