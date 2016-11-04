## Pandasticsearch = Elasticsearch + Pandas DataFrame

Pandasticsearch is a lightweight Elasticsearch client for data-analysis purpose. It interprets query results into
 [Pandas](http://pandas.pydata.org) DataFrame objects for data analysis. This can be used to gain direct insight
  from Elasticsearch's analysis result, e.g. multi-level nested aggregation. Elasticsearch is skilled 
  in real-time indexing, search and data-analysis. The results returned by Elasticsearch Rest API still
  require processing before data scientists can conduct an analysis on. 

To install:

```
pip3 install pandasticsearch
```

## Usage

### High Level API

A `DataFrame` object accesses Elasticsearch with high level API, like [elasticsearch-dsl-py](https://github.com/elastic/elasticsearch-dsl-py).

It is type-safe, easy-to-use and Pandas-flavored.

```python
# Create a DataFrame object
from pandasticsearch import DataFrame
df = DataFrame.from_es('http://localhost:9200', index='people')

# Print the schema(mapping) of the index
df.printSchema()
# company
# |-- employee
#   |-- name: {'index': 'not_analyzed', 'type': 'string'}
#   |-- age: {'type': 'integer'}
#   |-- gender: {'index': 'not_analyzed', 'type': 'string'}

# Inspect the columns
df.columns
#['name', 'age', 'gender']

# Get the column
df['name']
# Column('name')

# Filter
df.filter(df['age'] < 13).collect()
# [Row(age=12,gender='female',name='Alice'), Row(age=11,gender='male',name='Bob')]

# Projection
df.filter(df['age'] < 25).select('name', 'age').collect()
# [Row(age=12,name='Alice'), Row(age=11,name='Bob'), Row(age=13,name='Leo')]

# Print the rows into console
df.filter(df['age'] < 25).select('name').show(3)
# +------+
# | name |
# +------+
# | Alice|
# | Bob  |
# | Leo  |
# +------+

# Aggregation
df[df['gender'] == 'male'].agg(df['age'].avg).collect()
# [Row(avg(age)=12)]

# Sort
df.sort(df['age'].asc).select('name', 'age').collect()
#[Row(age=11,name='Bob'), Row(age=12,name='Alice'), Row(age=13,name='Leo')]

# Convert to Pandas object for subsequent analysis
df[df['gender'] == 'male'].agg(Avg('age')).to_pandas()
#    avg(age)
# 0        12

```



### Use with Another Python Client

Pandasticsearch can also be used with another full featured Python client:

* [elasticsearch-py](https://github.com/elastic/elasticsearch-py) (Official)
* [Elasticsearch-SQL](https://github.com/NLPchina/elasticsearch-sql)
* [pyelasticsearch](https://github.com/pyelasticsearch/pyelasticsearch)
* [pyes](https://github.com/aparo/pyes)


### Contruct query

```Python
from pandasticsearch import DataFrame, Avg
body = df[df['gender'] == 'male'].agg(Avg('age')).to_dict()
 
from elasticsearch import Elasticsearch, Select
result_dict = es.search(index="recruit", body=body)
```

### Parse result

```python
from elasticsearch import Elasticsearch
es = Elasticsearch('http://localhost:9200')
result_dict = es.search(index="recruit", body={"query": {"match_all": {}}})
Select.from_dict(result_dict).to_pandas()
```


## Related Articles

* [Spark and Elasticsearch for real-time data analysis](https://spark-summit.org/2015-east/wp-content/uploads/2015/03/SSE15-35-Leau.pdf)


## LICENSE
 
MIT
