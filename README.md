[![Build Status](https://travis-ci.org/greatjapa/heelflip.svg?branch=master)](https://travis-ci.org/greatjapa/heelflip)
[![license](https://img.shields.io/github/license/mashape/apistatus.svg?maxAge=2592000)](https://github.com/greatjapa/heelflip/blob/master/LICENCE)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.greatjapa/heelflip/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.github.greatjapa/heelflip)
[![codecov](https://codecov.io/gh/greatjapa/heelflip/branch/master/graph/badge.svg)](https://codecov.io/gh/greatjapa/heelflip)

**Heelflip** is an in-memory JSON aggregator for Java. Sometimes we just want to read a couple of JSON samples and get analytics information from them. Before throw it on a relational or NoSQL databases, it would be interesting if we can get some quick results just read them in our code.
 
## How to use
Considering the following bookstore JSON sample:
```javascript
{"name":"The Lightning Thief","author":"Rick Riordan","genre":"fantasy","inStock":true,"price":12.50,"pages":384}
{"name":"The Sea of Monsters","author":"Rick Riordan","genre":"fantasy","inStock":true,"price":6.49,"pages":304}
{"name":"Sophie's World","author":"Jostein Gaarder","genre":"fantasy","inStock":false,"price":3.07,"pages":64}
{"name":"Lucene in Action","author":"Michael McCandless","genre":"IT","inStock":true,"price":30.50,"pages":475}
```
We can read then as follows:
```java
try(InputStream stream = new FileInputStream("bookstore.json")){
    JsonAgg jsonAgg = new JsonAgg();
    jsonAgg.loadNDJSON(zipStream);
    
    ...
}
```
### Global Aggregations

Once you have a JsonAgg object we can get global aggregations (min, max and sum) doing as follows:
```java
FieldAgg priceAgg = agg.getFieldAgg("price");
popAgg.getMin(); // 3.07
popAgg.getMax(); // 30.50
popAgg.getSum(); // 52.56
```
Or counting their values (count and cardinality):
```java
FieldAgg genreAgg = agg.getFieldAgg("genre");
genreAgg.count();          // 4
genreAgg.cardinality();    // 2
genreAgg.count("fantasy"); // 3
```
### Group By Aggregations

We also can get group by aggregations doing as follows:
```java
GroupByAgg groupByAgg = agg.getGroupBy("price", "inStock");
FieldAgg priceBystockAgg = groupByAgg.groupBy("true");
priceBystockAgg.getMin(); // 6.49
priceBystockAgg.getMax(); // 30.50
priceBystockAgg.getSum(); // 49.49
```
or
```java
GroupByAgg groupByAgg = agg.getGroupBy("name", "inStock");
FieldAgg namesInStockAgg = groupByAgg.groupBy("true");

namesInStockAgg.distinctValues(); 
//"The Sea of Monsters"
//"Lucene in Action"
//"The Lightning Thief"
```
<under construction>

### Objects
For instance, the following JSON entry:
```javascript
{ "city" : "SPRINGFIELD", "loc" : {"lat": -72.577769, "long": 42.128848}, "pop" : 22115}
```
will be read as:
```javascript
{ "city" : "SPRINGFIELD", "loc.lat" -72.577769, "loc.long": 42.128848, "pop" : 22115}
```

### Arrays
For instance, the following JSON entry:
```javascript
{ "city" : "SPRINGFIELD", "loc" : [ -72.577769, 42.128848 ], "pop" : 22115}
```
will be read as:
```javascript
{ "city" : "SPRINGFIELD", "loc_0" : -72.577769, "loc_1": 42.128848, "pop" : 22115}
```
