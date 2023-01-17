FilesDB
=======

This is a database of files from package managers. Similar to apt-file etc, it allows you to list files in packages, find a package providing a file, or search from a file hash.

Currently PyPI is supported.

# Web API

The database is available via a Web API at `https://filesdb.remram.fr/`. Try for example:

* `https://filesdb.remram.fr/pypi/reprozip/files`
* `https://filesdb.remram.fr/files/sha256/1c354a2b5e634641c3cc7c2cb9d49a1e1b93a1b28e99ceb5bb51d2c48010e961`
* `https://filesdb.remram.fr/files/prefix/reprounzip/unpackers/vagrant`
* `https://filesdb.remram.fr/python/import/sklearn`

# Data

You can also download the whole data (compressed SQLite3 database): https://f004.backblazeb2.com/file/rr4-files/filesdb.sqlite3.zst
