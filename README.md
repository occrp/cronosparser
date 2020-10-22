# cronosparser

### Status

As of mid 2020, `cronosparser` is broken in the following ways:

* `cronosparser` will not run under Python 3. You may see a variety of
  error messages - but check first if you're using Python 2.

* `cronosparser` only ever successfully decoded around 20-30% of the
  databases we have, and only ones generated using Cronos 3. 

For both of these issues, we'd love to receive pull requests that address
them. We don't currently have a timeline or plan to fix the issues 
ourselves.

### About cronosparser

This repo contains some WIP code to reverse engineer Cronos database files
used by the Russian-language CronosPro/CronosPlus database system, as well
as a few sample databases.

### Missing functionality

* Can password protection be circumvented?

## References

* [Cronos Product Home Page](http://www.cronos.ru/)
* [Discussion of the file format](http://sergsv.narod.ru/cronos.htm)
