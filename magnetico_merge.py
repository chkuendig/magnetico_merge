#!/usr/bin/env python3

import sqlite3
import sys

import click
import tqdm

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


def merge_files(connection, old_torrent_id, new_torrent_id):
    for row in connection.execute(
        "SELECT * from merged_db.files where torrent_id = ?", (old_torrent_id,)
    ):
        connection.execute(
            "INSERT INTO files (torrent_id, size, path) VALUES (?, ?, ?)",
            (new_torrent_id, row["size"], row["path"]),
        )


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

    cursor.execute("BEGIN")
    for row in cursor.execute("SELECT * FROM merged_db.torrents"):
        click.echo(f"Merging torrents {row['id']}: ", nl=False)
        try:
            torrent_merge = connection.execute(
                "INSERT INTO torrents (info_hash, name, total_size, discovered_on, updated_on, n_seeders, n_leechers, modified_on) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (*row[1:],),
            )
            click.echo("OK, merging files: ", nl=False)
            # Now merge files
            merge_files(connection, row["id"], torrent_merge.lastrowid)
            click.echo("OK")
        except sqlite3.IntegrityError:
            print("Failed")

    connection.rollback()
    connection.close()


if __name__ == "__main__":
    main()
