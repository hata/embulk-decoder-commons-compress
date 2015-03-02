# Commons Compress decoder plugin for Embulk

This decoder plugin for Embulk supports various archive formats using [Apache Commons Compress](http://commons.apache.org/proper/commons-compress/) library.

## Overview

* **Plugin type**: decoder
* **Load all or nothing**: yes
* **Resume supported**: no

## Configuration

- **format**: An archive format like tar, zip, and so on. (string, optional, default: "")
  - The format type is one of supported formats by by [Apache Commons Compress](http://commons.apache.org/proper/commons-compress/).
  - Auto detect is used when there is no configuration. This can use for a single format. If a file format is solid compression like tar.gz, please set format config explicitly.
  

## Support formats

- **archive format**: ar, arj, cpio, dump, jar, 7z, tar, zip
  - This is an archive format. This plugin lists all files for processing by embulk.
- **compress format**: bzip2, deflate, gzip, lzma, pack200, snappy-framed, snappy-raw, xz, z
  - This compress format lists a file for processing by embulk.
- **solid compression format**: Need to set the config value explicitly.
  - tgz, tar.gz
  - tbz, tbz2, tb2, tar.bz2
  - taz, tz, tar.Z
  - tlz, tar.lz, tar.lzma
  - txz, tar.xz

## Example

- Use auto detection. This can use for 1 format like tar and zip. If you would like to use combined format like tar.gz, please set the format to your configuration file.

```yaml
in:
  type: any input plugin type
  decoders:
    - type: commons-compress
```

- Set a file format like tar explicit.

```yaml
in:
  type: any input plugin type
  decoders:
    - type: commons-compress
      format: tar
```

- Set a combined format.

```
in:
  type: any input plugin type
  decoders:
    - type: commons-compress
      format: tgz
```

## Build

```
$ ./gradlew gem
```

## Reference

- [Apache Commons Compress](http://commons.apache.org/proper/commons-compress/)


