[![Build Status](https://travis-ci.org/greatjapa/heelflip.svg?branch=master)](https://travis-ci.org/greatjapa/heelflip)
[![license](https://img.shields.io/github/license/mashape/apistatus.svg?maxAge=2592000)](https://github.com/greatjapa/heelflip/blob/master/LICENCE)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.greatjapa/heelflip/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.github.greatjapa/heelflip)
[![codecov](https://codecov.io/gh/greatjapa/heelflip/branch/master/graph/badge.svg)](https://codecov.io/gh/greatjapa/heelflip)

**Heelflip** is an in-memory JSON aggregator for Java. Sometimes we just want to read a couple of JSON samples and get analytics information from them. Before throw it on a relational or NoSQL databases, it would be interesting if we can get some quick results just read them in our code.

## Maven
Heelflip is available at the Central Maven Repository:

```xml
<dependency>
    <groupId>com.github.greatjapa</groupId>
    <artifactId>heelflip</artifactId>
    <version>1.0</version>
</dependency>
```

## How to use
Considering the following bookstore JSON sample:
```javascript
{"name":"The Odyssey",  "author":"Homer",          "genre":"poem",  "inStock":true, "price":12.50}
{"name":"The Godfather","author":"Mario Puzo",     "genre":"novel", "inStock":true, "price":6.49 }
{"name":"Moby-Dick",    "author":"Herman Melville","genre":"novel", "inStock":false,"price":3.07 }
{"name":"Emma",         "author":"Austen",         "genre":"novel", "inStock":true, "price":30.50}
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
FieldAgg priceAgg = jsonAgg.getFieldAgg("price");
popAgg.getMin(); // 3.07
popAgg.getMax(); // 30.50
popAgg.getSum(); // 52.56
```
Or counting their values (count and cardinality):
```java
FieldAgg genreAgg = jsonAgg.getFieldAgg("genre");
genreAgg.count();          // 4
genreAgg.cardinality();    // 2
genreAgg.count("novel");   // 3
```
### Group By Aggregations

We also can get group by aggregations doing as follows:
```java
GroupByAgg groupByAgg = jsonAgg.getGroupBy("price", "inStock");
FieldAgg priceBystockAgg = groupByAgg.groupBy("true");
priceBystockAgg.getMin(); // 6.49
priceBystockAgg.getMax(); // 30.50
priceBystockAgg.getSum(); // 49.49
```
or
```java
GroupByAgg groupByAgg = jsonAgg.getGroupBy("name", "inStock");
FieldAgg namesInStockAgg = groupByAgg.groupBy("true");

namesInStockAgg.distinctValues(); 
//"The Odyssey"
//"The Godfather"
//"Emma"
```
### Dump Report
You can also generate a report with all aggregations accumulated. You just need to do:
```java
jsonAgg.dumpReport("report", true);
```
This code snippet will create a directory in the following structure:
```
report
│   __missingGroupBy.json
└───name
│   │   __name.json
│   │   name_groupBy_author.json
│   │   name_groupBy_genre.json
│   │   name_groupBy_inStock.json
│   │   name_groupBy_price.json
└───author
└───genre
└───inStock
└───price
```

### How aggregation works over non-flat JSON
There is no problem if you have JSONs with array or nested objects. What Heelflip does is calculate aggregations over fields and their values. But the Heelflip API is based on field names and, for that, we need to rename JSON fields when arrays or objects appears.

For instance, the following JSON entry:
```javascript
{"name":"Steve", "age":30, "address":{"street":"8nd Street", "city":"New York"}}
```
will be read as:
```javascript
{"name":"Steve", "age":30, "address.street":"8nd Street", "address.city":"New York"}}
```
To retrieve the information about the field "city" we need to concatenate the field names as showed below:
```java
FieldAgg cityAgg = agg.getFieldAgg("address.city");
cityAgg.count();
```

Similarly, the following JSON entry with arrays:
```javascript
{ "city" : "SPRINGFIELD", "loc" : [ -72.577769, 42.128848 ], "pop" : 22115}
```
will be read as:
```javascript
{ "city" : "SPRINGFIELD", "loc_0" : -72.577769, "loc_1": 42.128848, "pop" : 22115}
```

**NOTE**: The examples above are used only to ilustrate how we rename field names for objects and arrays to generate unique names at aggregation time. It is importante to understand that we do not flatten the JSON.
