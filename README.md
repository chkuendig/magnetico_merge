Merge magnetico databases
=========================

Very simple script to merge databases created by [magnetico](https://github.com/boramalper/magnetico) >=0.8

It supports merging SQLite database into SQLite or PostgreSQL, or merging PostgreSQL into SQLite.
```
Usage: magnetico_merge.py [OPTIONS] MAIN_DB MERGED_DB

Options:
  --fast  Try to go faster, by deleting indices and constraints while
          importing. PostgreSQL only. This can be really slower if your
          databases overlapped a lot.

  --help  Show this message and exit.
```

The `MERGED_DB` will be merged into `MAIN_DB`.

It requires python >= 3.6 and [click](https://click.palletsprojects.com/en/7.x/)

If you need to import into or load from PostgreSQL, you need to have [psycopg2](https://www.psycopg.org/). Having [pgcopy](https://github.com/altaurog/pgcopy) is faster when a lot of rows are in the `files` table.

Have fun.
