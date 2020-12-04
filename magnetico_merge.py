#!/usr/bin/env python3

import sqlite3
import sys

import click


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


@click.command()
@click.argument("main-db")
@click.argument("merged-db")
def main(main_db, merged_db):
    click.echo(f"Merging {merged_db} into {main_db}")
    connection = sqlite3.connect(main_db)
    connection.row_factory = sqlite3.Row
    connection.text_factory = bytes

    cursor = connection.cursor()
    cursor.execute("ATTACH ? AS merged_db", (merged_db,))
    click.echo("Gathering database statistics: ", nl=False)

    cursor.execute("SELECT count(*) from merged_db.torrents")
    total_merged = cursor.fetchone()[0]
    cursor.execute(
        "SELECT name FROM pragma_table_info('files') WHERE name not in ('id', 'torrent_id')"
    )
    remaining_file_colums = [row[0].decode() for row in cursor]
    cursor.execute(
        "SELECT name FROM pragma_table_info('torrents') WHERE name not in ('id')"
    )
    remaining_torrent_colums = [row[0].decode() for row in cursor]

    click.echo(f"{total_merged} torrents to merge.")

    insert_files_statement = f"INSERT INTO files (torrent_id, {','.join(remaining_file_colums)}) SELECT ?, {','.join(remaining_file_colums)} FROM merged_db.files WHERE torrent_id = ?"
    insert_torrents_statement = f"INSERT INTO torrents ({','.join(remaining_torrent_colums)}) VALUES ({','.join('?' * len(remaining_torrent_colums))})"
    failed_count = 0

    cursor.execute("BEGIN")
    with click.progressbar(length=total_merged, width=0, show_pos=True) as bar:
        for i, row in enumerate(cursor.execute("SELECT * FROM merged_db.torrents")):
            if i % 1000 == 0:
                bar.update(1000)
            try:
                torrent_merge = connection.execute(
                    insert_torrents_statement, (*row[1:],)
                )
                # Now merge files
                connection.execute(
                    insert_files_statement, (torrent_merge.lastrowid, row["id"])
                )
            except sqlite3.IntegrityError:
                failed_count += 1

    click.echo("Comitting… ", nl=False)
    connection.commit()
    click.echo(
        f"OK. {total_merged} torrents processed. {failed_count} torrents were not merged due to errors."
    )
    connection.close()


if __name__ == "__main__":
    main()
