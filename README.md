# Commons Compress decoder plugin for Embulk

This decoder plugin for Embulk supports various archive formats using [Apache Commons Compress](http://commons.apache.org/proper/commons-compress/) library.

## Overview

* **Plugin type**: decoder
* **Load all or nothing**: yes
* **Resume supported**: no

## Configuration

- **format**: An archive format like tar and zip. (string, required)
  - The format type is one of supported formats by by [Apache Commons Compress](http://commons.apache.org/proper/commons-compress/).

## Example

```yaml
in:
  type: any input plugin type
  decoders:
    - type: commons-compress
      format: tar
```

## Build

```
$ ./gradlew gem
```

## Reference

- [Apache Commons Compress](http://commons.apache.org/proper/commons-compress/)


