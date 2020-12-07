#!/usr/bin/env python3

import sqlite3
import sys
import pathlib

from typing import Union, List, Dict, Tuple

import click

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    psycopg2 = None


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


class Merger:
    def __init__(self, main_db: str, merged_db: str):
        self.main_cursor = None
        self.merged_cursor = None
        self.main_connection = None
        self.merged_connection = None

        self.main_connection = self.connect(main_db)
        self.merged_connection = self.connect(
            merged_db,
            self.main_connection
            if isinstance(self.main_connection, sqlite3.Connection)
            else None,
        )

        self.main_cursor = self.main_connection.cursor()
        self.merged_cursor = (
            self.main_cursor
            if self.same_connection()
            else self.merged_connection.cursor()
        )
        self.merged_type = (
            "sqlite" if isinstance(self.merged_connection, sqlite3.Connection) else "pg"
        )
        self.main_type = (
            "sqlite" if isinstance(self.main_connection, sqlite3.Connection) else "pg"
        )

        self.prepare_inserts()

    def __del__(self):
        self.close()

    def close(self):
        if self.main_cursor is not None:
            self.main_cursor.close()
            self.main_cursor = None
        if self.merged_cursor is not None:
            self.merged_cursor.close()
            self.merged_cursor = None

        if self.main_connection is not None:
            self.main_connection.close()
            self.main_connection = None
        if self.merged_connection is not None:
            self.merged_connection.close()
            self.merged_connection = None

    def connect(self, db_dsn: str, existing_sqlite: sqlite3.Connection = None):
        if db_dsn.startswith("postgresql://"):
            if psycopg2 is None:
                raise click.ClickException("psycopg2 driver is missing")
            connection = psycopg2.connect(db_dsn)
        elif pathlib.Path(db_dsn).exists():
            if existing_sqlite is not None:
                with existing_sqlite:  # Shortcut available only in sqlite
                    existing_sqlite.execute("ATTACH ? AS merged_db", (db_dsn,))
                connection = existing_sqlite
            else:
                connection = sqlite3.connect(db_dsn)
                connection.row_factory = sqlite3.Row
                # Some name were inserted invalid. Correct them.
                connection.text_factory = lambda x: x.decode("utf8", errors="replace")
        else:
            raise click.ClickException(f"Unrecognized database {db_dsn}")

        return connection

    def same_connection(self):
        return self.main_connection is self.merged_connection

    def get_total_merged(self):
        if self.same_connection():
            self.merged_cursor.execute("SELECT count(*) from merged_db.torrents")
        else:
            self.merged_cursor.execute("SELECT count(*) from torrents")
        return self.merged_cursor.fetchone()[0]

    def get_file_columns(self):
        request = (
            "SELECT name FROM pragma_table_info('files') WHERE name not in ('id', 'torrent_id')"
            if self.main_type == "sqlite"
            else """SELECT column_name AS name FROM information_schema.columns
            WHERE table_name = 'files' and column_name not in ('id', 'torrent_id')"""
        )
        self.main_cursor.execute(request)
        return [row[0] for row in self.main_cursor]

    def get_torrent_columns(self):
        request = (
            "SELECT name FROM pragma_table_info('torrents') WHERE name not in ('id')"
            if self.main_type == "sqlite"
            else """SELECT column_name AS name FROM information_schema.columns
            WHERE table_name = 'torrents' and column_name not in ('id')"""
        )
        self.main_cursor.execute(request)
        return [row[0] for row in self.main_cursor]

    def prepare_inserts(self):
        self.torrent_columns = self.get_torrent_columns()
        if self.main_type == "pg":
            self.insert_torrents_statement = f"""INSERT INTO torrents ({','.join(self.torrent_columns)})
            VALUES %s ON CONFLICT DO NOTHING RETURNING id"""
        else:
            self.insert_torrents_statement = f"""INSERT INTO torrents ({','.join(self.torrent_columns)})
            VALUES ({','.join('?' * len(self.torrent_columns))}) ON CONFLICT DO NOTHING"""

        self.file_columns = self.get_file_columns()
        if self.same_connection():
            self.insert_files_statement = f"""INSERT INTO files (torrent_id, {','.join(self.file_columns)})
            SELECT ?, {','.join(self.file_columns)} FROM merged_db.files WHERE torrent_id = ?"""
        else:
            if self.main_type == "pg":
                # We will use execute_values with a single placeholder
                self.insert_files_statement = f"""INSERT INTO files (torrent_id, {','.join(self.file_columns)})
                VALUES %s ON CONFLICT DO NOTHING"""

    def select_merged_torrents(self, arraysize=1000):
        select_cursor = self.merged_connection.cursor()
        select_cursor.arraysize = arraysize
        select_cursor.execute(
            "SELECT * FROM merged_db.torrents"
            if self.same_connection()
            else "SELECT * FROM torrents"
        )
        return select_cursor

    def select_merged_files(self, torrent_id: int, arraysize=1000):
        select_cursor = self.merged_connection.cursor()
        select_cursor.arraysize = arraysize
        select_cursor.execute(
            f"SELECT * FROM files where torrent_id = {self.placeholder(self.merged_type)}",
            (torrent_id,),
        )
        return select_cursor

    def select_merged_files_m(self, torrent_ids: Tuple[int], arraysize=1000):
        select_cursor = self.merged_connection.cursor()
        select_cursor.arraysize = arraysize
        select_cursor.execute(
            f"""SELECT * FROM files WHERE torrent_id IN
            ({','.join([self.placeholder(self.merged_type)] * len(torrent_ids))})""",
            torrent_ids,
        )
        return select_cursor

    def merge_entries(
        self, torrents: List[dict]
    ) -> Dict[Union["inserted", "failed"], int]:
        if self.main_type == "pg":
            try:
                result = psycopg2.extras.execute_values(
                    self.main_cursor,
                    self.insert_torrents_statement,
                    [
                        (*[torrent[column] for column in self.torrent_columns],)
                        for torrent in torrents
                    ],
                    fetch=True,
                )
            except ValueError as e:
                if '0x00' in str(e):
                    for torrent in torrents:
                        torrent['name'] = torrent['name'].replace('\x00', '')
                    # Retry
                    return self.merge_entries(torrents)
            self.merge_files_m(
                {
                    torrent["id"]: one_result[0]
                    for (one_result, torrent) in zip(result, torrents)
                    if one_result[0] is not None
                }
            )
            return {"failed": result.count((None,)), "inserted": len(result)}
        elif self.main_type == "sqlite":
            failed = 0
            inserted = 0
            for torrent in torrents:
                self.main_cursor.execute(
                    self.insert_torrents_statement,
                    (*[torrent[column] for column in self.torrent_columns],),
                )
                inserted += 1
                if (
                    self.main_cursor.lastrowid is None
                    or self.main_cursor.lastrowid == 0
                ):
                    failed += 1
                else:
                    self.merge_files(self.main_cursor.lastrowid, torrent["id"])
            return {"failed": failed, "inserted": inserted}
        else:
            raise click.ClickException("Not implemented")

    def merge_files(self, torrent_id: int, previous_torrent_id: int):
        if self.same_connection():
            self.main_cursor.execute(
                self.insert_files_statement, (torrent_id, previous_torrent_id)
            )
        else:
            raise click.ClickException("Not supported")

    def merge_files_m(self, torrent_ids: Dict[int, int]):
        if self.main_type != "pg":
            raise click.ClickException("Not supported")
        select_cursor = self.select_merged_files_m(tuple(torrent_ids.keys()))
        files_list = select_cursor.fetchmany()
        while files_list:
            psycopg2.extras.execute_values(
                self.main_cursor,
                self.insert_files_statement,
                [
                    (
                        torrent_ids[merged_file["torrent_id"]],
                        *[merged_file[column] for column in self.file_columns],
                    )
                    for merged_file in files_list
                ],
            )
            files_list = select_cursor.fetchmany()

    def placeholder(self, db_type: str):
        return "?" if db_type == "sqlite" else "%s"


@click.command()
@click.argument("main-db")
@click.argument("merged-db")
def main(main_db, merged_db):
    click.echo(f"Merging (NEW) {merged_db} into {main_db}")
    merger = Merger(main_db, merged_db)
    click.echo("Gathering database statistics: ", nl=False)

    total_merged = merger.get_total_merged()
    click.echo(f"{total_merged} torrents to merge.")
    failed_count = 0

    merger.main_cursor.execute("BEGIN")
    arraysize = 1000
    with click.progressbar(length=total_merged, width=0, show_pos=True) as bar:
        torrents = merger.select_merged_torrents(arraysize)
        results = merger.merge_entries(torrents.fetchmany())
        while results["inserted"] > 0:
            bar.update(results["inserted"])
            failed_count += results["failed"]
            results = merger.merge_entries(torrents.fetchmany())

    click.echo("Comitting… ", nl=False)
    merger.main_connection.commit()
    click.echo(
        f"OK. {total_merged} torrents processed. {failed_count} torrents were not merged due to errors."
    )
    merger.close()


if __name__ == "__main__":
    main()
