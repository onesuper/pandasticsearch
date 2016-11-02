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

A `Pandasticsearch` object accesses Elasticsearch with high level API, like [elasticsearch-dsl-py](https://github.com/elastic/elasticsearch-dsl-py).


It is type-safe, easy-to-use and Pandas-flavored.

```python
# create a Pandasticsearch object
>>> from pandasticsearch import Pandasticsearch, Avg
>>> ps = Pandasticsearch('http://localhost:9200', index='company')
>>> ps.columns
['name', 'age', 'gender', 'intro']
>>> ps.printSchema()
company
|-- employee
  |-- name: {'index': 'not_analyzed', 'type': 'string'}
  |-- age: {'type': 'integer'}
  |-- gender: {'index': 'not_analyzed', 'type': 'string'}
  |-- intro: {'store': True, 'analyzer': 'ik', 'type': 'string'}

# filter 
>>> ps.filter(ps['age'] > 25).show(10)
Select: 10 rows
>>> ps[ps['age'] < 30].show(10)
Select: 10 rows

#  aggregation
>>> ps[ps['gender'] == 'male'].aggregate(Avg('age'))
Agg: 1 row
>>> _.to_pandas()
   avg(age)
0     27.423532
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

It can also talk to [Elasticsearch-SQL](https://github.com/NLPchina/elasticsearch-sql) (You need to install the plugin first):

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

## Analyze Data in ES

### Metric Aggregation

```python
>>> from pandasticsearch import *
>>> ps = Pandasticsearch('http://localhost:9200', index='company')

# count documents/values
>>> ps.aggregate(ValueCount('age'), CountStar())
>>> _.to_pandas()
   count(*)  value_count(age)
0   1168604                 1167497

# distinct counts
>>> ps.aggregate(Cardinality('name'))
>>> _.to_pandas()
   cardinality(name)
0             771665

# percentiles
>>> ps.aggregate(Percentiles('birthYear', percents=[25,50,75]))
>>> _.to_pandas()
     25.0    50.0    75.0
0  1983.0  1989.0  1993.0

# percentile ranks
>>> ps.aggregate(PercentileRanks('birthYear', values=[1990,1985]))
>>> _.to_pandas()
      1985.0     1990.0
0  31.678808  56.836569
```


### Multi-Dimensional Aggregation

```python
>>> client = RestClient('http://localhost:9200')
>>> agg = client.execute('''
    select COUNT(*) as f1, AVG(*) as f2
    from table_name
    group by agg_key1, agg_key2
    ''', query=Agg())
>>> agg
Agg: 4 rows
>>> df = agg.to_pandas()
>>> df
                    f1  f2
agg_key1 agg_key2
a        x         100   1
         y         200   2
b        x         300   3
         y         400   4
```

## Related Articles

* [Spark and Elasticsearch for real-time data analysis](https://spark-summit.org/2015-east/wp-content/uploads/2015/03/SSE15-35-Leau.pdf)


## LICENSE
 
MIT
