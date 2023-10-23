# MongoDB Change Streams Plugin for Benthos

<img src='https://github.com/usedatabrew/pglogicalstream/blob/main/images/databrew-logo.png' width="200px" align="middle" >

Welcome to the MongoDB Change Streams Plugin for Benthos! This plugin allows you to seamlessly stream data changes from your MongoDB database using Benthos, a versatile stream processor.

## Features

- **Real-time Data Streaming:** Capture data changes in real-time as they happen in your MongoDB database.

- **Flexible Configuration:** Easily configure the plugin to specify the database connection details

## Prerequisites

Before you begin, make sure you have the following prerequisites:

- [Benthos](https://github.com/Jeffail/benthos): Required to import into your Golang code

- [MongoDB](https://www.mongodb.com/): Change streams are available for replica sets and sharded clusters:

## Getting Started

To get started you have to run benthos with custom plugins. Since this plugin is not adopted by benthos itself 
you have to create a new benthos build with plugin registered

```go
package main

import (
	"github.com/benthosdev/benthos/v4/public/service"
	// import mongodb_stream plugin
	_ "github.com/usedatabrew/mongodb_stream_benthos"
)

func main() {
	// here we initialize benthos
	service.Run()
}
```

### Create benthos configuration with plugin

```yaml
input:
  label: mongodb_stream_input
  # register new plugin
  mongodb_stream:
    uri: database uri
    database: name of the database
    collection: collection name
    stream_snapshot: set true if you want to stream existing data. If set to false only a new data will be streamed
```