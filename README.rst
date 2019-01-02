FilesDB
=======

.. image:: https://img.shields.io/gitlab/pipeline/remram44/filesdb/master.svg
   :target: https://gitlab.com/remram44/filesdb/pipelines

This is a database of files from package managers. Similar to apt-file etc, it allows you to list files in packages, find a package providing a file, or search from a file hash.

Currently PyPI is supported.

Web API
-------

The database is available via a Web API at ``https://filesdb.reprozip.org/``. Try for example ``https://filesdb.reprozip.org/sha1/fd9f54bfefd0e410ec85fe7e920acbbbbd276f29``.
