# Bucket Recursor

Returns a lazy `Stream<S3ObjectSummary>`, which recursively queries prefix hierarchies in a s3
bucket as the stream is consumed. Applies filters during traversal so that only desired objects are
included.

`BucketRecursor` can:

* Find every object at a specified prefix depth.
* Find objects from the past n days, hours, etc. when prefixes include time information (date, hour,
etc.).
* Find objects where the prefix matches a regex. E.g. messages where "src" and "dst are different:
"messages/src=(\w+)/dst=(?!\1)\w+/".
* Minimize memory footprint by using the pagination feature of s3 list api.

# Dependency Coordinates

gradle

```
compile "org.jmdware:s3-bucket-recursor:1.0.1"
```

leiningen

```
[org.jmdware/s3-bucket-recursor "1.0.1"]
```

maven

```xml
<dependency>
    <groupId>org.jmdware</groupId>
    <artifactId>s3-bucket-recursor</artifactId>
    <version>1.0.1</version>
</dependency>
```

This library uses [slf4j](https://www.slf4j.org/), replacing commons-logging with the slf4j
[shim](https://www.slf4j.org/legacy.html).

# Usage

For example, a s3 bucket contains the following prefixes and objects (gz files):

```
action-reports/
    ...
activity-logs/
    date=20190415/
        hour=22/
            ...
        hour=23/
            orders/
                orders1.gz
                orders2.gz
                ...
            views/
                views1.gz
                ...
    date=20190416/
        hour=1/
            orders/
                ...
            ...
        ...
```

The easiest way to get all the orders and views objects is to start recursion at "activity-logs/",
the start prefix for this traversal, and recurse to prefix depth 3:

1. date=yyyyMMdd/
1. date=yyyyMMdd/hour=H/
1. date=yyyyMMdd/hour=H/orders/ and date=yyyyMMdd/hour=H/views/

```java
BucketRecursor recursor = new BucketRecursor(
    amazonS3,         // AmazonS3 instance from aws-java-sdk-s3.jar
    bucket,
    '/',              // prefix delimiter
    "activity-logs",  // start prefix, depth 0, where recursion begins
    pageSize,         // max results per call to s3 list objects api

    // three Predicate<String>
    Filters.anything(),  // accept any prefix at depth 1: activity-logs/date=yyyyMMdd/
    Filters.anything(),  // accept any prefix at depth 2: activity-logs/date=yyyyMMdd/hour=H/
    Filters.anything()); // accept any prefix at depth 3: activity-logs/date=yyyyMMdd/hour=H/orders/ and activity-logs/date=yyyyMMdd/hour=H/views/

// equivalently, specify the "anything" depth
BucketRecursor recursor = new BucketRecursor(
    amazonS3,
    bucket,
    '/',
    "activity-logs",
    pageSize,
    3);              // prefix recursion depth limit

Stream<S3ObjectSummary> stream = recursor.stream();

stream.forEach(downloader::download);
```

Starting from the given prefix, `BucketRecursor` lists prefixes and applies a filter at each depth.
Each filter is a `Predicate<String>`, which receives the *relative* prefix (to the start prefix).
Where a predicate returns `true`, `BucketRecursor` will recurse another prefix level.

Alternatively, we could have traversed the entire bucket by passing `null` as the start prefix. In
this case, we would need four filters, the first of which should accept only "activity-logs/". The
rest should accept anything.

```java
BucketRecursor recursor = new BucketRecursor(
    amazonS3,
    bucket,
    '/',
    null,     // null start prefix so start recursion at the top level of the bucket
    pageSize,

    // four Predicate<String> filters
    "activity-logs/"::equals, // depth 1: only recurse into "activity-logs/".
    anything(),               // depth 2: activity-logs/date=yyyyMMdd/
    anything(),               // depth 3: activity-logs/date=yyyyMMdd/hour=H/
    anything());              // depth 4: activity-logs/date=yyyyMMdd/hour=H/orders/ and activity-logs/date=yyyyMMdd/hour=H/views/
```

See [Filters](src/main/java/org/jmdware/s3br/Filters.java) for helper methods for creating
`List<Predicate<String>>`.

A more instructive example is finding all orders data from
2019-04-15T23:00:00 or later (see [Filters.Builder](src/main/java/org/jmdware/s3br/Filters.java)).
This type of traversal is complicated by how the criteria depends on multiple parts of the prefix
hierachy to decide whether to recurse deeper.

Orders data are three prefixes below "activity-logs/". So, to find orders data from
2019-04-15T23:00:00 or later, we need three filters:

1. "date=yyyyMMdd/" where yyyyMMdd is 2015-04-15 or later.
1. "date=yyyyMMdd/hour=H/" where yyyyMMdd H is 2015-04-15T23 or later.
1. "date=yyyyMMdd/hour=H/orders/" where yyyyMMdd H is 2015-04-15T23 or later.

Starting with "activity-logs/", recursion traverses the prefix hierarchy in the following,
depth-first order:

|Filter Argument (prefix relative to start)|Depth|Filter 1|Filter 2|Filter 3|
|----|----|----|----|----|
|date=20190415/|1|Y| | |
|date=20190415/hour=22/|2| |N| |
|date=20190415/hour=23/|2| |Y| |
|date=20190415/hour=23/orders/|3| | |Y|
|date=20190415/hour=23/views/|3| | |N|
|date=20190416/|1|Y| | |
|date=20190416/hour=0/|2| |Y| |
|date=20190416/hour=0/orders/|3| | | Y|
|date=20190416/hour=0/views/|3| | |N|
|...| | | | |

("Depth" is depth from the start prefix)

The `Stream<S3ObjectSummary>` will include all s3 objects having prefixes accepted at depth = 3, the
number of filters.

Note how filter 1 (counting from 1) is only invoked at depth 1, filter 2 at depth 2, and filter 3 at
depth 3. Filter n is only ever invoked at depth n.

Using [Filters.Builder](src/main/java/org/jmdware/s3br/Filters.java),
construct the `BucketRecursor` like so:

```java
BucketRecursor recursor = new BucketRecursor(
    amazonS3,
    bucket,
    '/',
    "activity-logs",
    pageSize,
    new Filters.Builder('/')
            .after("2019-04-15T23:00:00Z")

            // add DateTimeFormatter pattern for the date level in the prefix hierarchy
            .addLevelTemporal("'date='yyyyMMdd")

            // add DateTimeFormatter pattern for the hour level in the prefix hierarchy
            .addLevelTemporal("'hour='H")

            // add regex pattern for the orders
            .addLevelRegex("orders")

            // returns List<Predicate<String>> with size 3
            .toFilters());

Stream<S3ObjectSummary> stream = recursor.stream();

stream.filter(not(history::wasDownloaded)).forEach(downloader::download);
```

# Common Problems

Be sure to setup the recursion depth correctly. For example, the regular expression
".+/src=(\/w+)/dst=(?!\1).+/$" matches prefixes where "src" and "dst" are different. The
following, however, will _not_ work:

```java
BucketRecursor recursor = new BucketRecursor(
    amazonS3,
    bucket,
    '/',
    "starting-prefix/",
    pageSize,
    Pattern.compile(".+/src=(\\w+)/dst=(?!\\1).+/$").asPredicate()); // applied to prefixes at depth 1
```

The `Pattern` is the _first_ filter, which applies at depth _1_. But recursion must hit three levels
deep before the pattern can match. It needs at least three levels because it starts with a wildcard
and then compares the penultimate prefix, "src", with the last prefix, "dst". A hint is the logging,
which, if enabled, will include:

```
INFO  org.jmdware.s3br.BucketRecursor          - recursively listing 'starting-prefix/' to prefix depth 1
```

Note the "to prefix depth 1" at the end of the log message.

To apply the regex at depth 3, you must ensure prefixes at depths 1 and 2 are accepted:

```java
BucketRecursor recursor = new BucketRecursor(
    amazonS3,
    bucket,
    '/',
    null,
    pageSize,
    Filters.anything(),                                              // depth 1
    Filters.anything(),                                              // depth 2: src
    Pattern.compile(".+/src=(\\w+)/dst=(?!\\1).+/$").asPredicate()); // depth 3: src vs dst
```

Then the log will include:

```
INFO  org.jmdware.s3br.BucketRecursor          - recursively listing 'starting-prefix/' to prefix depth 3
```
