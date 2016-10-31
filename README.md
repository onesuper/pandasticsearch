## Pandasticsearch = Pandas + Elasticsearch

Pandasticsearch is a lightweight Elasticsearch client for data-analysis purpose. It interprets query results into
 [Pandas](http://pandas.pydata.org) objects for data analysis. This can be used to gain direct insight
  from Elasticsearch's analysis result, e.g. multi-level nested aggregation. Elasticsearch is skilled 
  in real-time indexing, search and data-analysis. The results returned by Elasticsearch Rest API still
  require processing before data scientists can conduct an analysis on. 

To install:

```
pip3 install pandasticsearch
```

## Connect to ES

### High Level API (Experimental)

A `Pandasticsearch` object comes with a bunch of high level APIs out of box:

```python
>>> from pandasticsearch import Pandasticsearch, col, max
>>> ps = Pandasticsearch('http://localhost:9200', index='company')

# filter & top
>>> ps.where(col('birthYear') == 1990).top()
...
... 
...

# filter & aggregation
>>> ps.where(col('department') == 'finance').aggregate(avg('birthYear'))
>>> _.to_pandas()
   avg(birthYear)
0     1986.227061

# value count
>>> ps.value_count('age')
>>> _.to_pandas()
   value_count(name)
0              1501780

# distinct count 
>>> ps.distinct_count('name')
>>> _.to_pandas()
   value_count(name)
0             771665

# percentiles
>>> ps.percentiles('birthYear', percents=[25,50,75])
>>> _.to_pandas()
     25.0    50.0    75.0
0  1983.0  1989.0  1993.0

# percentile ranks
>>> ps.percentile_ranks('birthYear', values=[1990,1985])
>>> _.to_pandas()
      1985.0     1990.0
0  31.678808  56.836569
```

### SqlClient (Recommended)

A `SqlClient` talks to [Elasticsearch-SQL](https://github.com/NLPchina/elasticsearch-sql) (You need to install the plugin first):

```python
from pandasticsearch.client import SqlClient

client = SqlClient('http://localhost:9200')
query = client.execute('select * from table_name')
print(query.json)
```

### RestClient (Minimal Dependency)

A `RestClient` talks to default Elasticsearch Rest API :

```python
from pandasticsearch.client import RestClient

client = RestClient('http://localhost:9200', 'recruit/resume/_search')
query = client.execute("query":{"match_all":{}}})
print(query.json)
```

### Used with Other Python Client

Pandasticsearch can also be used with another full featured Python client:

* [elasticsearch-py](https://github.com/elastic/elasticsearch-py) (Official)
* [pyelasticsearch](https://github.com/pyelasticsearch/pyelasticsearch)
* [pyes](https://github.com/aparo/pyes)

```python
from elasticsearch import Elasticsearch
from pandasticsearch.query import Select

es = Elasticsearch('http://localhost:9200')
result_dict = es.search(index="recruit", body={"query": {"match_all": {}}})

select = Select()
select.explain_result(result_dict)
df = select.to_pandas()
```

## Analyze Data in ES

### Selection

```python
>>> client = SqlClient('http://localhost:9200')
>>> select = client.execute('select a,b from table_name limit 3', query=Select())
>>> select
values: [{'a': 1, 'b': 1}, {'a': 2, 'b': 2}, {'a': 3, 'b': 3}]
>>> df = select.to_pandas()
>>> df
   a  b
0  1  1
1  2  2
2  3  3
```

### Groupby (Aggregation)

```python
>>> client = SqlClient('http://localhost:9200')
>>> agg = client.execute('select COUNT(*) as f from table_name group by agg_key', query=Agg())
>>> agg
>>> df = agg.to_pandas()
>>> df
index_names: ('agg_key',)
indexes: [('a',), ('b',)]
values: [{'f1': 100}, {'f1': 200}]
          f1
agg_key
a        100
b        200
```

### Multi-Dimensional Aggregation

```python
>>> client = SqlClient('http://localhost:9200')
>>> agg = client.execute('''
    select COUNT(*) as f1, AVG(*) as f2
    from table_name
    group by agg_key1, agg_key2
    ''', query=Agg())
>>> agg
index_names: ('agg_key1', 'agg_key2')
indexes: [('a', 'x'), ('a', 'y'), ('b', 'x'), ('b', 'y')]
values: [{'f2': 1, 'f1': 100}, {'f2': 2, 'f1': 200}, {'f2': 3, 'f1': 300}, {'f2': 4, 'f1': 400}]
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
