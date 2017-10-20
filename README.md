# Mongoose HDFS storage driver

# Notes

Node's FS browser is available at default port #50070

HDFS default port #9000

## Basic Testing

1. Run the pseudo distributed HDFS cluster
```bash
docker run -d --net host -e SSH_PORT=2222 --name hdfs dockerq/docker-hdfs
```

2. Open the browser and check the HDFS share @ http://127.0.0.1:50070/explorer.html
Here the data will be observable.

3. Build the Mongoose HDFS storage driver jar either the Docker image.

4. Put the HDFS storage driver jar into the Mongoose's `ext` directory
either use the Docker image with HDFS support.

5. Run some Mongoose test, for example:
```bash
java -jar mongoose-4.0.0/mongoose.jar \
    --item-data-size=64MB \
    --item-output-file=hdfs.files.csv \
    --item-output-path=/test \
    --storage-auth-uid=root \
    --storage-driver-concurrency=10 \
    --storage-driver-type=hdfs \
    --storage-net-node-addr=<HADOOP_NAME_NODE_IP_ADDR> \
    --storage-net-node-port=9000 \
    --test-step-limit-count=100
```

# Operations

The information below describes which particular methods are invoked
on the endpoint in each case. The endpoint hereafter is a
[Hadoop FileSystem](https://hadoop.apache.org/docs/r2.8.0/api/org/apache/hadoop/fs/FileSystem.html)
instance.

The item types `data` and `path` are supported.
`token` type is not supported.

## Data

Operations on the data are implemented as file operations

### Noop

Doesn't invoke anything.

### Create

If size is 0 then `createNewFile(Path f)` is invoked (returning
true/false).

Otherwise, `create(Path f, false, int bufferSize)` is invoked with
calculated output buffer size. The returned `FSDataOutputStream` is
used to write the data.

#### Copy

Uses both `create` and `open` methods to obtain output and input streams

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

`create(Path f, true, int bufferSize)` is invoked with
calculated output buffer size. The returned `FSDataOutputStream` is
used to write the data.

#### Random Ranges

`create(Path f, true, int bufferSize)` is invoked with
calculated output buffer size. The returned `FSDataOutputStream` is
used to write the data at the calculated positions.

#### Fixed Ranges

`create(Path f, true, int bufferSize)` is invoked with
calculated output buffer size. The returned `FSDataOutputStream` is
used to write the data at the calculated positions.

##### Append

`append(Path f, int bufferSize)` is invoked.

### Delete

`delete(Path f, false)` is invoked.

## Path

Operations on the path are implemented as directory operations

### Create

`mkdirs(Path)`

#### Copy

TODO

#### Concatenation

TODO

### Read

`listFiles(Path f, false)` is invoked returning the `RemoteIterator`
instance which is used to iterate the directory contents.

### Delete

`delete(Path f, true)` is invoked.

## Token

Not supported
