## Overview

MemKV is a in-memory Key value store backed by append only file with following mechanism.

- Get is served from in-memory Go's map implemenation
- Set updates the map as well as append to file.
- Remove removes the entry from Map, appends delete entry to file
- Depends on OS to synchronize the file in normal cases
- Sync method force syncs the file
- Optimize() optimizes the file to trim its size
- Entire file is read during Open to populate the in-memory map
- Not tested extensively, but it can be used by single process and multiple threads

## Example

	SEE memkv_test.go

## TODO

- Autooptimize
