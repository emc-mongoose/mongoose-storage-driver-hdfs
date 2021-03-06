[![Gitter chat](https://badges.gitter.im/emc-mongoose.png)](https://gitter.im/emc-mongoose)
[![Issue Tracker](https://img.shields.io/badge/Issue-Tracker-orange.svg)](https://mongoose-issues.atlassian.net/projects/GOOSE)
[![CI status](https://gitlab.com/emc-mongoose/mongoose-storage-driver-hdfs/badges/master/pipeline.svg)](https://gitlab.com/emc-mongoose/mongoose-storage-driver-hdfs/commits/master)
[![Maven metadata URL](https://img.shields.io/maven-metadata/v/http/central.maven.org/maven2/com/github/emc-mongoose/mongoose-storage-driver-hdfs/maven-metadata.xml.svg)](http://central.maven.org/maven2/com/github/emc-mongoose/mongoose-storage-driver-hdfs)
[![Docker Pulls](https://img.shields.io/docker/pulls/emcmongoose/mongoose-storage-driver-hdfs.svg)](https://hub.docker.com/r/emcmongoose/mongoose-storage-driver-hdfs/)

[Mongoose](https://github.com/emc-mongoose/mongoose-base)'s HDFS storage
driver

# Introduction

The storage driver extends the Mongoose's Abstract NIO Storage Driver
and uses the following libraries:
* hadoop-common
* hadoop-hdfs-client

# Features

* Authentification: simple
* SSL/TLS - TODO
* Item types:
    * `data`
    * `path` - TODO
* Path listing input
* Automatic destination path creation on demand
* Data item operation types:
    * `create`, additional modes:
        * copy
    * `read`
        * full
        * random byte ranges
        * fixed byte ranges
        * content verification
    * `update`
        * full (overwrite)
        * fixed byte ranges: append mode only
    * `delete`
    * `noop`
* Path item operation types (TODO):
    * `create`, additional modes:
        * copy - ?
        * concatenation - ?
    * `read`
    * `delete`
    * `noop`

# Usage

Get the latest pre-built jar file which is available at:
http://repo.maven.apache.org/maven2/com/github/emc-mongoose/mongoose-storage-driver-hdfs/
The jar file may be downloaded manually and placed into the `<USER_HOME_DIR>/.mongoose/<VERSION>/ext`
directory of Mongoose to be automatically loaded into the runtime.

```bash
java -jar mongoose-<VERSION>.jar \
    --storage-driver-type=hdfs \
    --storage-net-node-addrs=<NODE_IP_ADDRS> \
    --storage-net-node-port=<NODE_PORT> \
    --storage-auth-uid=<USER_ID> \
    ...
```

## Docker

### Standalone

```bash
docker run \
    --network host \
    emcmongoose/mongoose-storage-driver-hdfs \
    --storage-net-node-addrs=<NODE_IP_ADDRS> \
    --storage-net-node-port=<NODE_PORT> \
    --storage-auth-uid=<USER_ID> \
    ...
```

### Distributed

#### Additional Node

```bash
docker run \
    --network host \
    --expose 1099 \
    emcmongoose/mongoose-storage-driver-hdfs
    --run-node
```

#### Entry Node

```bash
docker run \
    --network host \
    emcmongoose/mongoose-storage-driver-hdfs \
    --load-step-node-addrs=<ADDR1,ADDR2,...> \
    --storage-net-node-addrs=<NODE_IP_ADDRS> \
    --storage-net-node-port=<NODE_PORT> \
    --storage-auth-uid=<USER_ID> \
    ...
```

## Advanced

### Sources

```bash
git clone https://github.com/emc-mongoose/mongoose-storage-driver-hdfs.git
cd mongoose-storage-driver-hdfs
```

### Test

```
./gradlew clean test
```

### Build

```bash
./gradlew clean jar
```

### Embedding

```groovy
compile group: 'com.github.emc-mongoose', name: 'mongoose-storage-driver-hdfs', version: '<VERSION>'
```

### Development Notes

Node's FS browser is available at default port #50070

HDFS default port #8020 (or 9000?)

#### Basic Testing

1. Run the pseudo distributed HDFS cluster
```bash
docker run -p 22022:22 -p 8020:8020 -p 50010:50010 -p 50020:50020 -p 50070:50070 -p 50075:50075 -d pravega/hdfs
```

2. Open the browser and check the HDFS share @ http://127.0.0.1:50070/explorer.html
to observe the filesystem

3. Build the Mongoose HDFS storage driver jar either use the Docker image.

4. Put the HDFS storage driver jar into the Mongoose's `ext` directory
either use the Docker image with HDFS support.

5. Run some Mongoose test, for example:
```bash
java -jar mongoose-<VERSION>.jar \
    --item-data-size=64MB \
    --item-output-file=hdfs.files.csv \
    --item-output-path=/test \
    --load-op-limit-count=100 \
    --storage-auth-uid=root \
    --storage-driver-limit-concurrency=10 \
    --storage-driver-type=hdfs \
    --storage-net-node-addrs=<HADOOP_NAME_NODE_IP_ADDR> \
    --storage-net-node-port=8020
```

### Operations

The information below describes which particular methods are invoked
on the endpoint in each case. The endpoint hereafter is a
[Hadoop FileSystem](https://hadoop.apache.org/docs/r2.9.0/api/org/apache/hadoop/fs/FileSystem.html)
instance.

The item types `data` and `path` are supported.
`token` type is not supported.

#### Data

Operations on the data items type are implemented as file operations

##### Noop

Doesn't invoke anything.

##### Create

The method `create(Path, FsPerm, boolean, int, short, long, null)` is invoked with
calculated output buffer size. The returned `FSDataOutputStream` is
used to write the data.

###### Copy

Uses both `create` and `open` methods to obtain output and input streams

###### Concatenation

Note: not supported as far as HDFS doesn't allow to concatenate to the new & empty destination object

`concat(Path dst, Path[] srcs)` is invoked (doesn't return anything).

Note:
> source files ranges concatenation is not supported.

##### Read

`open(Path f, int bufferSize)` is invoked. The returned
`FSDataInputStream` instance is used then to read the data.

###### Partial

The same method used as above, because the `FSDataInputStream` supports
the positioning needed for the partial read.

**Random Ranges**

Supported

**Fixed Ranges**

Supported

##### Update

###### Overwrite

Supported

###### Random Ranges

Not supported as far as [FSDataOutputStream](http://hadoop.apache.org/docs/r2.9.0/api/org/apache/hadoop/fs/FSDataOutputStream.html)
doesn't allow positioning.

###### Fixed Ranges

Not supported except the append case.

**Append**

Supported

##### Delete

`delete(Path f, false)` is invoked.

#### Path

Operations on the path are implemented as directory operations

##### Create

`mkdirs(Path)`

###### Copy

TODO

###### Concatenation

TODO

##### Read

`listFiles(Path f, false)` is invoked returning the `RemoteIterator`
instance which is used to iterate the directory contents.

##### Delete

`delete(Path f, true)` is invoked.

#### Token

Not supported
