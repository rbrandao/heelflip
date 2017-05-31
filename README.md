# heelflip
[![Build Status](https://travis-ci.org/greatjapa/heelflip.svg?branch=master)](https://travis-ci.org/greatjapa/heelflip)
[![license](https://img.shields.io/github/license/mashape/apistatus.svg?maxAge=2592000)](https://github.com/greatjapa/heelflip/blob/master/LICENCE)
[![codecov](https://codecov.io/gh/greatjapa/heelflip/branch/master/graph/badge.svg)](https://codecov.io/gh/greatjapa/heelflip)

##### Objects
For instance, the following JSON entry:
```javascript
{ "city" : "SPRINGFIELD", "loc" : {"lat": -72.577769, "long": 42.128848}, "pop" : 22115}
```
will be readed as:
```javascript
{ "city" : "SPRINGFIELD", "loc.lat" -72.577769, "loc.long": 42.128848, "pop" : 22115}
```

##### Arrays
For instance, the following JSON entry:
```javascript
{ "city" : "SPRINGFIELD", "loc" : [ -72.577769, 42.128848 ], "pop" : 22115}
```
will be readed as:
```javascript
{ "city" : "SPRINGFIELD", "loc_0" : -72.577769, "loc_1": 42.128848, "pop" : 22115}
```
