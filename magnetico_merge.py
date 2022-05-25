#!/usr/bin/env python3

from __future__ import annotations

import sqlite3
import sys
import pathlib

from abc import ABC, abstractmethod
from functools import cached_property
from typing import Union, Optional, List, Dict, Tuple

import click

try:
    import psycopg2
    import psycopg2.extras
    import psycopg2.errorcodes
except ImportError:
    psycopg2 = None

try:
    import pgcopy
except ImportError:
    pgcopy = None

"""
Schema:
CREATE TABLE torrents (
            id             INTEGER PRIMARY KEY,
            info_hash      BLOB NOT NULL UNIQUE,
            name           TEXT NOT NULL,
            total_size     INTEGER NOT NULL CHECK(total_size > 0),
            discovered_on  INTEGER NOT NULL CHECK(discovered_on > 0)
        ,
         updated_on INTEGER CHECK (updated_on > 0) DEFAULT NULL,
          n_seeders  INTEGER CHECK ((updated_on IS NOT NULL AND n_seeders >= 0) OR (updated_on IS NULL AND n_seeders IS NULL)) DEFAULT NULL,
           n_leechers INTEGER CHECK ((updated_on IS NOT NULL AND n_leechers >= 0) OR (updated_on IS NULL AND n_leechers IS NULL)) DEFAULT NULL,
            modified_on INTEGER NOT NULL
                CHECK (modified_on >= discovered_on AND (updated_on IS NOT NULL OR modified_on >= updated_on))
                DEFAULT 32503680000);
CREATE TABLE files (
            id          INTEGER PRIMARY KEY,
            torrent_id  INTEGER REFERENCES torrents ON DELETE CASCADE ON UPDATE RESTRICT,
            size        INTEGER NOT NULL,
            path        TEXT NOT NULL
        , is_readme INTEGER CHECK (is_readme IS NULL OR is_readme=1) DEFAULT NULL, content   TEXT    CHECK ((content IS NULL AND is_readme IS NULL) OR (content IS NOT NULL AND is_readme=1)) DEFAULT NULL);
CREATE UNIQUE INDEX info_hash_index ON torrents	(info_hash);
CREATE UNIQUE INDEX readme_index ON files (torrent_id, is_readme);
CREATE VIRTUAL TABLE torrents_idx USING fts5(name, content='torrents', content_rowid='id', tokenize="porter unicode61 separators ' !""#$%&''()*+,-./:;<=>?@[\]^_`{|}~'")
/* torrents_idx(name) */;
CREATE TABLE IF NOT EXISTS 'torrents_idx_data'(id INTEGER PRIMARY KEY, block BLOB);
CREATE TABLE IF NOT EXISTS 'torrents_idx_idx'(segid, term, pgno, PRIMARY KEY(segid, term)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS 'torrents_idx_docsize'(id INTEGER PRIMARY KEY, sz BLOB);
CREATE TABLE IF NOT EXISTS 'torrents_idx_config'(k PRIMARY KEY, v) WITHOUT ROWID;
CREATE TRIGGER torrents_idx_ai_t AFTER INSERT ON torrents BEGIN
              INSERT INTO torrents_idx(rowid, name) VALUES (new.id, new.name);
            END;
CREATE TRIGGER torrents_idx_ad_t AFTER DELETE ON torrents BEGIN
              INSERT INTO torrents_idx(torrents_idx, rowid, name) VALUES('delete', old.id, old.name);
            END;
CREATE TRIGGER torrents_idx_au_t AFTER UPDATE ON torrents BEGIN
              INSERT INTO torrents_idx(torrents_idx, rowid, name) VALUES('delete', old.id, old.name);
              INSERT INTO torrents_idx(rowid, name) VALUES (new.id, new.name);
            END;
CREATE TRIGGER "torrents_modified_on_default_t" AFTER INSERT ON "torrents" BEGIN
              UPDATE "torrents" SET "modified_on" = NEW."discovered_on" WHERE "id" = NEW."id" AND NEW."modified_on" = 32503680000;
            END;
CREATE INDEX modified_on_index ON torrents (modified_on);
"""

Connection = Union[sqlite3.Connection, psycopg2._psycopg.connection]
Cursor = Union[sqlite3.Cursor, psycopg2._psycopg.cursor]


class Database(ABC):
    def __init__(self, dsn: str, source: Database = None):
        self.cursor: Optional[Cursor] = None
        self.connection: Optional[Connection] = None
        self.source = source

        self.connection = self.connect(dsn)
        self.cursor = self.connection.cursor()

        self.torrents_table = "torrents"
        self.options = {}

    @classmethod
    def from_dsn(cls, dsn: str, source: Database = None) -> Database:
        if dsn.startswith("postgresql://"):
            return PostgreSQL(dsn, source)
        elif pathlib.Path(dsn).exists():
            return SQLite(dsn, source)

    def __del__(self):
        self.close()

    def close(self):
        if getattr(self, "cursor", None) is not None:
            self.cursor.close()
            self.cursor = None
        if getattr(self, "connection", None) is not None:
            self.connection.close()
            self.connection = None

    def set_options(self, options: dict = {}):
        self.options = options

    @abstractmethod
    def connect(self, dsn: str) -> Connection:
        pass

    def before_import(self):
        pass

    def after_import(self):
        pass

    @property
    def stripped_files_constraint(self) -> str:
        if self.options.get("stripped_files", False):
            return f"WHERE (SELECT count(*) FROM files WHERE {self.torrents_table}.id = files.torrent_id) > 0"
        return ""

    @property
    def torrents_count(self) -> int:
        self.cursor.execute(
            f"SELECT count(*) from {self.torrents_table} {self.stripped_files_constraint}"
        )
        return self.cursor.fetchone()[0]

    @property
    @abstractmethod
    def file_columns(self) -> list[str]:
        pass

    @property
    @abstractmethod
    def torrent_columns(self) -> list[str]:
        pass

    def get_torrents_cursor(self, arraysize=1000) -> Cursor:
        select_cursor = self.select_cursor(arraysize)
        select_cursor.execute(
            f"SELECT * FROM {self.torrents_table} {self.stripped_files_constraint}"
        )
        return select_cursor

    def select_cursor(self, arraysize: int) -> Cursor:
        select_cursor = self.connection.cursor()
        select_cursor.arraysize = arraysize
        return select_cursor

    @abstractmethod
    def merge_torrents(
        self, torrents: list[dict]
    ) -> dict[Union["inserted", "failed"], int]:
        pass

    @property
    @abstractmethod
    def placeholder(self):
        pass


class SQLite(Database):
    def __init__(self, filename: str, source: SQLite = None):
        self.source = None
        if source is not None and not isinstance(source, (SQLite, PostgreSQL)):
            raise ValueError("SQLite target can only use SQLite or PostgreSQL source")
        super().__init__(filename, source)
        # For type hints
        self.source = source

    def close(self):
        if not self.merged_source:
            super().close()

    def connect(self, dsn: str) -> sqlite3.Connection:
        if self.merged_source:
            with self.source.connection:  # Shortcut available only in sqlite
                self.source.connection.execute(f"ATTACH ? AS {self.target_db}", (dsn,))
            return self.source.connection
        else:
            connection = sqlite3.connect(dsn)
            connection.row_factory = sqlite3.Row
            # Some name were inserted invalid. Correct them.
            connection.text_factory = lambda x: x.decode("utf8", errors="replace")
            return connection

    @cached_property
    def file_columns(self):
        self.cursor.execute(
            "SELECT name FROM pragma_table_info('files') WHERE name not in ('id', 'torrent_id')"
        )
        return [row[0] for row in self.cursor]

    @cached_property
    def torrent_columns(self):
        self.cursor.execute(
            "SELECT name FROM pragma_table_info('torrents') WHERE name not in ('id')"
        )
        return [row[0] for row in self.cursor]

    @property
    def target_db(self):
        return "target_db" if self.merged_source else "main"

    def merge_torrents(
        self, torrents: list[dict]
    ) -> dict[Union["inserted", "failed"], int]:
        torrents_statement = f"""INSERT INTO {self.target_db}.torrents ({','.join(self.source.torrent_columns)})
            VALUES ({','.join('?' * len(self.source.torrent_columns))}) ON CONFLICT DO NOTHING"""
        files_statement = f"""INSERT INTO {self.target_db}.files (torrent_id, {','.join(self.source.file_columns)})
        SELECT ?, {','.join(self.source.file_columns)} FROM files WHERE torrent_id = ?"""

        failed = 0
        inserted = 0
        processed = 0
        for torrent in torrents:
            self.cursor.execute(
                torrents_statement,
                (*[torrent[column] for column in self.source.torrent_columns],),
            )
            processed += 1
            if self.cursor.lastrowid is None or self.cursor.lastrowid == 0:
                failed += 1
            else:
                inserted += 1
                self.merge_files(files_statement, self.cursor.lastrowid, torrent["id"])

        return {
            "failed": failed,
            "inserted": inserted,
            "processed": processed,
            "last": torrents[-1] if len(torrents) > 0 else None,
        }

    def merge_files(self, statement: str, torrent_id: int, previous_torrent_id: int):
        if self.merged_source:
            self.cursor.execute(statement, (torrent_id, previous_torrent_id))

    @property
    def merged_source(self):
        # Class is tested in __init__
        return isinstance(self.source, SQLite)

    @property
    def placeholder(self):
        return "?"


class PostgreSQL(Database):
    def __init__(self, dsn: str, source: SQLite = None):
        if source is not None and not isinstance(source, SQLite):
            raise NotImplemented("PostgreSQL target can only use SQLite source")
        super().__init__(dsn, source)
        # For type hints
        self.source = source
        self.indices = {}
        self.create_contraint_statements = []
        self.drop_contraint_statements = []
        self.copy_manager = None
        self.named_cursor_count = 0

    def connect(self, dsn: str) -> psycopg2._psycopg.connection:
        if psycopg2 is None:
            raise click.ClickException("psycopg2 driver is missing")
        return psycopg2.connect(dsn, cursor_factory=psycopg2.extras.DictCursor)

    def generate_constraint_statements(self):
        # See https://blog.hagander.net/automatically-dropping-and-creating-constraints-131/
        self.cursor.execute(
            """
            SELECT 'ALTER TABLE '||nspname||'.\"'||relname||'\" ADD CONSTRAINT \"'
                    ||conname||'\" '|| pg_get_constraintdef(pg_constraint.oid)||';'
            FROM pg_constraint
            INNER JOIN pg_class ON conrelid=pg_class.oid
            INNER JOIN pg_namespace ON pg_namespace.oid=pg_class.relnamespace
            WHERE relname IN ('torrents', 'files') AND conname != 'torrents_info_hash_key'
            ORDER BY CASE WHEN contype='f' THEN 0 ELSE 1 END DESC,
                            contype DESC, nspname DESC, relname DESC, conname DESC"""
        )
        self.create_contraint_statements = [
            result[0] for result in self.cursor.fetchall()
        ]

        self.cursor.execute(
            """
            SELECT 'ALTER TABLE "'||nspname||'"."'||relname||'" DROP CONSTRAINT "'||conname||'";'
            FROM pg_constraint
            INNER JOIN pg_class ON conrelid=pg_class.oid
            INNER JOIN pg_namespace ON pg_namespace.oid=pg_class.relnamespace
            WHERE relname IN ('torrents', 'files') AND conname != 'torrents_info_hash_key'
            ORDER BY CASE WHEN contype='f' THEN 0 ELSE 1 END, contype, nspname, relname, conname"""
        )
        self.drop_contraint_statements = [
            result[0] for result in self.cursor.fetchall()
        ]

    def get_indices(self):
        self.cursor.execute(
            """SELECT indexname, indexdef FROM pg_indexes
                        WHERE schemaname = 'public'
                            AND tablename IN ('torrents', 'files')
                            AND indexname != 'torrents_info_hash_key'"""
        )
        return {result[0]: result[1] for result in self.cursor.fetchall()}

    def before_import(self):
        with self.connection:
            self.generate_constraint_statements()

            if self.options.get("fast", False):
                click.echo(
                    f"-> Postgresql target, dropping constraints…",
                    nl=False,
                )
                for statement in self.drop_contraint_statements:
                    self.cursor.execute(statement)
                click.echo(" Done.")

                # Collect indices after dropping constraints
                self.indices = self.get_indices()
                index_names = self.indices.keys()
                click.echo(
                    f"-> Postgresql target, dropping indices: {', '.join(index_names)}…",
                    nl=False,
                )
                self.cursor.execute(f"DROP INDEX {','.join(index_names)}")
                click.echo(" Done.")

            if pgcopy is not None:
                self.copy_manager = pgcopy.CopyManager(
                    self.connection, "files", ["torrent_id", *self.file_columns]
                )
            else:
                click.secho("pgcopy not found, so it won't be as fast", fg="yellow")

    def after_import(self):
        with self.connection:
            if self.options.get("fast", False):
                click.echo(
                    f"-> Postgresql target, recreating indices: {', '.join(self.indices.keys())}…",
                    nl=False,
                )
                for statement in self.indices.values():
                    self.cursor.execute(statement)
                click.echo(" Done.")

                click.echo(
                    f"-> Postgresql target, recreating constraints…",
                    nl=False,
                )
                for statement in self.create_contraint_statements:
                    self.cursor.execute(statement)
                click.echo(" Done.")

    @cached_property
    def file_columns(self):
        self.cursor.execute(
            """SELECT column_name AS name FROM information_schema.columns
            WHERE table_name = 'files' and column_name not in ('id', 'torrent_id')"""
        )
        return [row[0] for row in self.cursor]

    @cached_property
    def torrent_columns(self):
        self.cursor.execute(
            """SELECT column_name AS name FROM information_schema.columns
            WHERE table_name = 'torrents' and column_name not in ('id')"""
        )
        return [row[0] for row in self.cursor]

    def select_cursor(self, arraysize: int) -> Cursor:
        self.named_cursor_count += 1
        select_cursor = self.connection.cursor(name=f"select_{self.named_cursor_count}")
        select_cursor.arraysize = arraysize
        return select_cursor

    def merge_torrents(
        self, torrents: list[dict]
    ) -> dict[Union["inserted", "failed"], int]:
        torrents_statement = f"""INSERT INTO torrents ({','.join(self.torrent_columns)})
            VALUES %s ON CONFLICT DO NOTHING RETURNING id"""
        # We will use execute_values with a single placeholder
        files_statement = f"""INSERT INTO files (torrent_id, {','.join(self.file_columns)})
            VALUES %s ON CONFLICT DO NOTHING"""
        try:
            result = psycopg2.extras.execute_values(
                self.cursor,
                torrents_statement,
                [
                    (*[torrent[column] for column in self.torrent_columns],)
                    for torrent in torrents
                ],
                fetch=True,
            )
        except ValueError as e:
            if "0x00" in str(e):
                return self.merge_torrents(self.fix_bytes(torrents, "name"))
        self.merge_files(
            files_statement,
            {
                torrent["id"]: one_result[0]
                for (one_result, torrent) in zip(result, torrents)
                if one_result[0] is not None
            },
        )
        total = len(torrents)
        inserted = len(result)
        return {
            "failed": total - inserted,
            "inserted": inserted,
            "processed": total,
            "last": torrents[-1] if len(torrents) > 0 else None,
        }

    def merge_files(self, statement: str, torrent_ids: Dict[int, int]):
        if self.copy_manager is not None:
            try:
                self.cursor.execute("SAVEPOINT copy_files")
                self.copy_manager.threading_copy(self.get_source_files(torrent_ids))
            except psycopg2.DataError as error:
                if error.pgcode == psycopg2.errorcodes.CHARACTER_NOT_IN_REPERTOIRE:
                    self.cursor.execute("ROLLBACK TO copy_files")
                    self.copy_manager.threading_copy(
                        self.get_source_files(torrent_ids, True)
                    )
                else:
                    raise

            self.cursor.execute("RELEASE SAVEPOINT copy_files")
        else:
            # Slow path, but try to be as fast as possible
            files_cursor = self.get_source_files_cursor(tuple(torrent_ids.keys()))
            files_list = files_cursor.fetchmany()
            while files_list:
                try:
                    psycopg2.extras.execute_values(
                        self.cursor,
                        statement,
                        [
                            (
                                torrent_ids[merged_file["torrent_id"]],
                                *[merged_file[column] for column in self.file_columns],
                            )
                            for merged_file in files_list
                        ],
                    )
                    files_list = files_cursor.fetchmany()
                except ValueError as e:
                    if "0x00" in str(e):
                        files_list = self.fix_bytes(files_list, "path")

    def get_source_files(self, torrent_ids: Dict[int, int], fix_nul=False):
        files_cursor = self.get_source_files_cursor(tuple(torrent_ids.keys()))
        files_list = files_cursor.fetchmany()
        while files_list:
            if fix_nul:
                files_list = self.fix_bytes(files_list, "path")
            for merged_file in files_list:
                yield (
                    torrent_ids[merged_file["torrent_id"]],
                    *[merged_file[column] for column in self.file_columns],
                )
            files_list = files_cursor.fetchmany()

    def get_source_files_cursor(self, torrent_ids: Tuple[int], arraysize=1000):
        select_cursor = self.source.connection.cursor()
        select_cursor.arraysize = arraysize
        select_cursor.execute(
            f"""SELECT * FROM files WHERE torrent_id IN
            ({','.join([self.source.placeholder] * len(torrent_ids))})""",
            torrent_ids,
        )
        return select_cursor

    def fix_bytes(self, rows: List[dict], column: str):
        click.secho(
            f"Fix bytes value from {len(rows)} rows in column {column}", fg="yellow"
        )
        # Make a dict copy to be able to modify it
        rows = [dict(row) for row in rows]
        for row in rows:
            old = row[column]
            if isinstance(old, bytes):
                old = old.decode("utf8", errors="replace")
            row[column] = old.replace("\x00", "")
        return rows

    @property
    def placeholder(self):
        return "%s"


@click.command()
@click.option(
    "--fast",
    is_flag=True,
    help="Try to go faster, by deleting indices and constraints while importing. PostgreSQL only. This can be really slower if your databases overlapped a lot.",
)
@click.option(
    "--stripped-files",
    is_flag=True,
    help="Assume that the merged database has a stripped down 'files' table, and only merge torrents that have at least some files.",
)
@click.argument("main-db")
@click.argument("merged-db")
def main(main_db, merged_db, fast, stripped_files):
    click.echo(f"Merging {merged_db} into {main_db}")
    try:
        source = Database.from_dsn(merged_db)
        target = Database.from_dsn(main_db, source)
    except Exception as e:
        raise click.ClickException(e)
        return
    source.set_options({"stripped_files": stripped_files})
    target.set_options({"fast": fast})

    click.echo("-> Gathering source database statistics: ", nl=False)

    total_merged = source.torrents_count
    click.echo(f"{total_merged} torrents to merge.")
    failed_count = 0

    click.echo("-> Preparing target database")
    target.before_import()

    try:
        target.cursor.execute("BEGIN")
        arraysize = 1000
        click.echo(f"-> Starting transaction, iterating with {arraysize} torrents")
        with click.progressbar(length=total_merged, width=0, show_pos=True) as bar:
            torrents = source.get_torrents_cursor(arraysize)
            results = target.merge_torrents(torrents.fetchmany())
            while results["processed"] > 0:
                bar.update(results["processed"])
                failed_count += results["failed"]
                results = target.merge_torrents(torrents.fetchmany())

            if stripped_files:
                last_id = results["last"]["id"]
                click.echo(
                    f"Last merged torrent id is {last_id}. You can strip your merged database with 'DELETE FROM files WHERE torrent_id <= '{last_id}''"
                )

        click.echo("Comitting… ", nl=False)
        target.connection.commit()
        click.echo(
            f"OK. {total_merged} torrents processed. {failed_count} torrents were not merged due to errors."
        )
    except BaseException as e:
        click.secho(f"Error while importing {str(e)}", fg="red")
        target.connection.rollback()
        import traceback

        traceback.print_exc()
    finally:
        target.after_import()

    source.close()
    target.close()


if __name__ == "__main__":
    main()
