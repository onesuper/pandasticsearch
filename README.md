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

## Connect to ES

### High Level API

A `DataFrame` object accesses Elasticsearch with high level API, like [elasticsearch-dsl-py](https://github.com/elastic/elasticsearch-dsl-py).


It is type-safe, easy-to-use and Pandas-flavored.

```python
# create a DataFrame object
>>> from pandasticsearch import DataFrame
>>> df = DataFrame.from_es('http://localhost:9200', index='people')
>>> df.columns
['name', 'age', 'gender']
>>> df.printSchema()
company
|-- employee
  |-- name: {'index': 'not_analyzed', 'type': 'string'}
  |-- age: {'type': 'integer'}
  |-- gender: {'index': 'not_analyzed', 'type': 'string'}

# filter
>>> df.filter(df['age'] < 25).collect()
[Row(age=12,gender='female',name='Alice'), Row(age=11,gender='male',name='Bob'), Row(age=13,gender='male',name='Leo')]

# projection
>>> df.filter(df['age'] < 25).select('name', 'age').collect()
[Row(age=12,name='Alice'), Row(age=11,name='Bob'), Row(age=13,name='Leo')]

# print the rows into console
>>> df.filter(df['age'] < 25).select('name').show(3)
+------+
| name |
+------+
| Alice|
| Bob  |
| Leo  |
+------+

# aggregation
>>> from pandasticsearch import Avg
>>> df[df['gender'] == 'male'].agg(Avg('age')).collect()
[Row(avg(age)=12)]

# convert to Pandas object for subsequent analysis
>>> df[df['gender'] == 'male'].agg(Avg('age')).to_pandas()
   avg(age)
0        12
```


### RestClient

A `RestClient` talks to default Elasticsearch Rest API:

```python
>>> from pandasticsearch import RestClient, Select
>>> client = RestClient('http://localhost:9200', 'recruit/resume/_search')
>>> result_dict = client.post("query":{"match_all":{}}})
>>> Select.from_dict(result_dict)
Select: 3 rows
```

It can also talk to [Elasticsearch-SQL](https://github.com/NLPchina/elasticsearch-sql):

```python
>>> client = RestClient('http://localhost:9200', '_sql')
>>> result_dict = client.post(params={'sql': 'select * from table_name limit 3'})
>>> Select.from_dict(result_dict)
Select: 3 rows
```

### Use with Another Python Client

Pandasticsearch can also be used with another full featured Python client:

* [elasticsearch-py](https://github.com/elastic/elasticsearch-py) (Official)
* [pyelasticsearch](https://github.com/pyelasticsearch/pyelasticsearch)
* [pyes](https://github.com/aparo/pyes)

```python
>>> from elasticsearch import Elasticsearch, Select
>>> es = Elasticsearch('http://localhost:9200')
>>> result_dict = es.search(index="recruit", body={"query": {"match_all": {}}})
>>> Select.from_dict(result_dict)
Select: 10 rows
```


## Related Articles

* [Spark and Elasticsearch for real-time data analysis](https://spark-summit.org/2015-east/wp-content/uploads/2015/03/SSE15-35-Leau.pdf)


## LICENSE
 
MIT
