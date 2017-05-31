[![Build Status](https://travis-ci.org/greatjapa/heelflip.svg?branch=master)](https://travis-ci.org/greatjapa/heelflip)
[![license](https://img.shields.io/github/license/mashape/apistatus.svg?maxAge=2592000)](https://github.com/greatjapa/heelflip/blob/master/LICENCE)
[![codecov](https://codecov.io/gh/greatjapa/heelflip/branch/master/graph/badge.svg)](https://codecov.io/gh/greatjapa/heelflip)

**Heelflip** is an in-memory JSON aggregator for Java. Sometimes we just want to read a couple of JSON samples and get analytics information from them. Before throw it on a relational or NoSQL databases, it would be interesting if we can get some quick results just read them in our code.
 
#### How to use
Considering the following zip code JSON sample:
```javascript
{ "city" : "CHICOPEE",     "pop" : 23396, "state" : "MA", "_id" : "01013" }
{ "city" : "CHICOPEE",     "pop" : 31495, "state" : "MA", "_id" : "01020" }
{ "city" : "WESTOVER AFB", "pop" : 1764,  "state" : "MA", "_id" : "01022" }
{ "city" : "JARRELL",      "pop" : 3430,  "state" : "TX", "_id" : "76537" }
{ "city" : "JONESBORO",    "pop" : 793,   "state" : "TX", "_id" : "76538" }
{ "city" : "KEMPNER",      "pop" : 3884,  "state" : "TX", "_id" : "76539" }
```
We can read then as follows:
```java
try(InputStream stream = new FileInputStream("zips.json")){
    JsonAgg jsonAgg = new JsonAgg();
    jsonAgg.loadNDJSON(zipStream);
    
    ...
}
```
After that we can get global aggregations doing as follows:
```java
FieldAgg popAgg = jsonAgg.getFieldAgg("pop");
popAgg.getMin(); // 793
popAgg.getMax(); // 31495
popAgg.getSum(); // 64762
```

And group by aggregations doing as follows:
```java
GroupAgg popByStateAgg = jsonAgg.getGroupBy("pop", "state");
FieldAgg popByMA = popByStateAgg.groupBy("MA");
popByMA.getMin(); // 1764
popByMA.getMax(); // 31495
popByMA.getSum(); // 56655
```


<under construction>

##### Objects
For instance, the following JSON entry:
```javascript
{ "city" : "SPRINGFIELD", "loc" : {"lat": -72.577769, "long": 42.128848}, "pop" : 22115}
```
will be read as:
```javascript
{ "city" : "SPRINGFIELD", "loc.lat" -72.577769, "loc.long": 42.128848, "pop" : 22115}
```

##### Arrays
For instance, the following JSON entry:
```javascript
{ "city" : "SPRINGFIELD", "loc" : [ -72.577769, 42.128848 ], "pop" : 22115}
```
will be read as:
```javascript
{ "city" : "SPRINGFIELD", "loc_0" : -72.577769, "loc_1": 42.128848, "pop" : 22115}
```
