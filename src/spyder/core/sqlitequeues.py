#
# Copyright (c) 2010 Daniel Truemper truemped@googlemail.com
#
# sqlitequques.py 24-Jan-2011
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# under the License.
# All programs in this directory and
# subdirectories are published under the GNU General Public License as
# described below.
#
#
"""
Implement an URI Queue stored in SQLite.

Within this module an URI is represented as a tuple

   uri = (url, etag, mod_date, queue, next_date)
"""

import sqlite3 as sqlite


class SQLiteStore(object):
    """
    Simple store managing the simple sqlite stuff.
    """

    def __init__(self, db_name):
        """
        Initialize the sqlite store.

        `db_name` can be a filanem or `:memory:` for in-memory databases.
        """
        self._connection = sqlite.connect(db_name)
        self._connection.row_factory = sqlite.Row
        self._connection.execute("PRAGMA encoding=\"UTF-8\";")
        self._connection.execute("PRAGMA locking_mode=EXCLUSIVE;")

    def close(self):
        """
        Close the SQLite connection.
        """
        self._connection.close()


class SQLiteUriQueues(SQLiteStore):
    """
    A queue of uris.
    """

    def __init__(self, db_name):
        """
        Initialize the simple uri queue.

        This is a single queue working only with one host!
        """
        SQLiteStore.__init__(self, db_name)

        # create the tables if they do not exist
        self._connection.executescript("""
                CREATE TABLE IF NOT EXISTS queues(
                    url TEXT PRIMARY KEY ASC,
                    etag TEXT,
                    mod_date INTEGER,
                    queue INTEGER,
                    next_date INTEGER
                );

                CREATE INDEX IF NOT EXISTS queue_fifo ON queues(
                    queue ASC,
                    next_date ASC
                );
                """)

    def add_uri(self, uri):
        """
        Add a :class:`CrawlUri` to the specified queue.
        """
        (url, etag, mod_date, queue, next_date) = uri
        self._connection.execute("""INSERT INTO queues
                (url, etag, mod_date, queue, next_date) VALUES
                (?, ?, ?, ?, ?)""", (url, etag, mod_date, queue, next_date))

    def add_uris(self, urls):
        """
        Add a list of uris.
        """
        self._connection.executemany("""INSERT INTO queues
                (url, etag, mod_date, queue, next_date) VALUES
                (?, ?, ?, ?, ?)""", urls)

    def update_uri(self, uri):
        """
        Update the uri.
        """
        (url, etag, mod_date, queue, next_date) = uri
        self._connection.execute("""UPDATE queues SET
                etag=?, mod_date=?, queue=?, next_date=?
                WHERE url=?""", (etag, mod_date, queue, next_date, url))

    def update_uris(self, uris):
        """
        Update the list of uris in the database.
        """
        update_uris = [(etag, mod_date, queue, next_date, url)
            for (url, etag, mod_date, queue, next_date) in uris]
        self._connection.executemany("""UPDATE queues SET
                etag=?, mod_date=?, queue=?, next_date=?
                WHERE url=?""", update_uris)

    def queue_head(self, queue, n=1, offset=0):
        """
        Return the top `n` elements from the queue. By default, return the top
        element from the queue.

        If you specify `offset` the first `offset` entries are ignored.
        """
        cursor = self._connection.execute("""SELECT * FROM queues
                WHERE queue=?
                ORDER BY next_date ASC
                LIMIT ?
                OFFSET ?""", (queue, n, offset))
        results = []
        for row in cursor:
            results.append((row['url'], row['etag'], row['mod_date'],
                row['queue'], row['next_date']))

        return results

    def remove_uris(self, uris):
        """
        Remove all uris.
        """
        del_uris = [(url,) for (url, etag, mod_date, queue, next_date) in uris]
        self._connection.executemany("DELETE FROM queues WHERE url=?",
                del_uris)
