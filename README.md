# Mongoose HDFS storage driver

# Notes

Node's FS browser is available at default port #50070

HDFS default port #8020 either #9000

# Operations

The information below describes which particular methods are invoked
on the endpoint in each case. The endpoint hereafter is a
[Hadoop FileSystem](https://hadoop.apache.org/docs/r2.8.0/api/org/apache/hadoop/fs/FileSystem.html)
instance.

The item types `data` and `path` are supported.
`token` type is not supported.

## Data Operations

### Noop

Doesn't invoke anything.

### Create

If size is 0 then `createNewFile(Path f)` is invoked (returning
true/false).

Otherwise, `create(Path f, false, bufferSize)` is invoked with
calculated output buffer size. The returned `FSDataOutputStream` is
used to write the data.

#### Copy

`createSymlink(Path dst, Path src, true)` is invoked (doesn't return
anything).

#### Concatenation

`concat(Path dst, Path[] srcs)` is invoked (doesn't return anything).

Note:
> source files ranges concatenation is not supported.

### Read

`open(Path f, int bufferSize)` is invoked. The returned
`FSDataInputStream` instance is used then to read the data.

#### Partial

The same method used as above, because the `FSDataInputStream` supports
the positioning needed for the partial read.

##### Random Ranges

No additional info.

##### Fixed Ranges

No additional info.

### Update

#### Overwrite



#### Random Ranges

#### Fixed Ranges

##### Append

### Delete

`delete(Path f, false)` is invoked.

## Path Operations

### Create

`mkdirs(Path)`

### Delete

`delete(Path f, true)` is invoked.
