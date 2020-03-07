# `dynamo`

`dynamo` is a dead-simple CLI for AWS DynamoDB.

![Demo video of usage](https://user-images.githubusercontent.com/369053/51354899-4bb01f80-1b09-11e9-954d-957194d0b004.gif)

## Installation

* Mac: `brew install glassechidna/taps/dynamo`
* Windows: `scoop bucket add glassechidna https://github.com/glassechidna/scoop-bucket.git; scoop install dynamo`
* Otherwise get the latest build from the [Releases][releases] tab.

## Usage

```
dynamo [-n COUNT] [--dax CLUSTER] table-name [partition key value [sort key value-or-expression]]
```

* If only a table name is specified, every row will be scanned.
* If a table name and partition key are provided, either:
  * That item will be returned (for single-key schemas)
  * All matching items will be returned (for partition+sort key schemas)
* If a table name, partition key and sort key are provided, only the matching
  item will be returned.

In place of a constant sort key, you can also type:

* `> someval` - all items with sort key value greater than `someval`
* `>= someval` - as above, but "greater than or equal to"
* `<= someval` - as above, but "less than or equal to"
* `< someval` - as above, but "less than"
* `between val1 val2` - all items with sort keys between `val1` and `val2`
* `someval*` - all items with sort keys that begin with `someval`

By default, only **10** items will be returned -- this is to avoid accidentally
downloading an entire table! This can be controlled with the `-n 30` flag.
Passing `-n 0` disables the limit.

DynamoDB Accelerator (DAX) clusters are also supported. Pass `--dax cluster-name`
or `--dax cluster-address:port` to take advantage of this.

Finally, output is pretty-printed and colourised when executed directly. If
executed as part of a script (as determined by `isatty`), output will be
one-item-per-line in [JSON Lines][jsonlines] format.

[releases]: https://github.com/glassechidna/dynamo/releases
[jsonlines]: http://jsonlines.org/
