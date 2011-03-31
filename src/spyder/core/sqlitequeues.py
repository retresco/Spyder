#
# Copyright (c) 2010 Daniel Truemper truemped@googlemail.com
#
# sqlitequques.py 24-Jan-2011
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""
This module contains the default queue storages backed by SQlite.
"""
import sqlite3 as sqlite


class QueueException(Exception):
    """
    Base exception for errors in the queues.
    """
    pass

class UriNotFound(QueueException):
    """
    Exception raised when an URI could not be found in the storage.
    """

    def __init__(self, url):
        self._url = url

    def __repr__(self):
        return "UriNotFound(%s)" % (self._url,)


class QueueNotFound(QueueException):
    """
    Exception raised when a ``queue`` could not be found.
    """

    def __init__(self, identifier):
        self._identifier = identifier

    def __repr__(self):
        return "QueueNotFound(%s)" % (self._identifier,)


class SQLiteStore(object):
    """
    Simple base class for sqlite based queue storages. This class basically
    creates the default pragmas and initializes all the unicode stuff.
    """

    def __init__(self, db_name):
        """
        Initialize the sqlite store.

        `db_name` can be a filanem or `:memory:` for in-memory databases.
        """
        self._connection = sqlite.connect(db_name)
        self._connection.row_factory = sqlite.Row
        self._connection.text_factory = sqlite.OptimizedUnicode
        self._cursor = self._connection.cursor()
        self._cursor.execute("PRAGMA encoding=\"UTF-8\";")
        self._cursor.execute("PRAGMA locking_mode=EXCLUSIVE;")

    def close(self):
        """
        Close the SQLite connection.
        """
        self.checkpoint()
        self._connection.close()

    def checkpoint(self):
        """
        Checkpoint the database, i.e. commit everything.
        """
        self._connection.commit()


class SQLiteSingleHostUriQueue(SQLiteStore):
    """
    This is a queue that can be used for crawling a single host.

    Internally there is only one queue for all URLs. Each URL is represented as
    a tuple of the form: ``uri = (url, etag, mod_date, next_date, priority)``.

    The queue is ordered using the ``next_date`` in ascending fashion.
    """

    def __init__(self, db_name):
        """
        Initialize the simple uri queue.

        This is a single queue working only with one host!
        """
        SQLiteStore.__init__(self, db_name)

        # create the tables if they do not exist
        self._cursor.executescript("""
                CREATE TABLE IF NOT EXISTS queue(
                    url TEXT PRIMARY KEY ASC,
                    etag TEXT,
                    mod_date INTEGER,
                    next_date INTEGER,
                    priority INTEGER
                );

                CREATE INDEX IF NOT EXISTS queue_fifo ON queue(
                    next_date ASC,
                    priority ASC
                );
                """)

    def add_uri(self, uri):
        """
        Add a uri to the specified queue.
        """
        (url, etag, mod_date, next_date, prio) = uri
        self._cursor.execute("""INSERT INTO queue
                (url, etag, mod_date, next_date, priority) VALUES
                (?, ?, ?, ?, ?)""", (url, etag, mod_date, next_date, prio))

    def add_uris(self, urls):
        """
        Add a list of uris.
        """
        self._cursor.executemany("""INSERT INTO queue
                (url, etag, mod_date, next_date, priority) VALUES
                (?, ?, ?, ?, ?)""", urls)

    def update_uri(self, uri):
        """
        Update the uri.
        """
        (url, etag, mod_date, next_date, prio) = uri
        self._cursor.execute("""UPDATE queue SET
                etag=?, mod_date=?, next_date=?, priority=?
                WHERE url=?""", (etag, mod_date, next_date, prio, url))

    def update_uris(self, uris):
        """
        Update the list of uris in the database.
        """
        update_uris = [(etag, mod_date, next_date, priority, url)
            for (url, etag, mod_date, next_date, priority) in uris]
        self._cursor.executemany("""UPDATE queue SET
                etag=?, mod_date=?, next_date=?, priority=?
                WHERE url=?""", update_uris)

    def ignore_uri(self, url, status):
        """
        Called when an URI should be ignored. This is usually the case when
        there is a HTTP 404 or recurring HTTP 500's.
        """
        self.update_uri((url, None, None, status, 1))

    def queue_head(self, n=1, offset=0):
        """
        Return the top `n` elements from the queue. By default, return the top
        element from the queue.

        If you specify `offset` the first `offset` entries are ignored.

        Any entries with a `next_date` below `1000` are being ignored. This
        enables the crawler to ignore URIs _and_ storing the status code.
        """
        self._cursor.execute("""SELECT * FROM queue
                WHERE next_date > 1000
                ORDER BY next_date ASC
                LIMIT ?
                OFFSET ?""", (n, offset))
        for row in self._cursor:
            yield (row['url'], row['etag'], row['mod_date'],
                row['next_date'], row['priority'])

    def remove_uris(self, uris):
        """
        Remove all uris.
        """
        del_uris = [(url,) for (url, _etag, _mod_date, _queue, _next_date)
            in uris]
        self._cursor.executemany("DELETE FROM queue WHERE url=?",
                del_uris)

    def __len__(self):
        """
        Calculate the number of known uris.
        """
        cursor = self._cursor.execute("""SELECT count(url) FROM queue""")
        return cursor.fetchone()[0]

    def all_uris(self):
        """
        A generator for iterating over all available urls.

        Note: does not return the full uri object, only the url. This will be
        used to refill the unique uri filter upon restart.
        """
        self._cursor.execute("""SELECT url FROM queue""")
        for row in self._cursor:
            yield row['url']

    def get_uri(self, url):
        """
        Mostly for debugging purposes.
        """
        self._cursor.execute("SELECT * FROM queue WHERE url=?",
                (url,))
        row = self._cursor.fetchone()
        if row:
            return (row['url'], row['etag'], row['mod_date'], row['next_date'],
                    row['priority'])
        raise UriNotFound(url)


class SQLiteMultipleHostUriQueue(SQLiteStore):
    """
    A queue storage for multiple queues that can be used for crawling multiple
    hosts simultaneously.

    Internally all URLs are being stored in one table. Each queue has its own
    INTEGER identifier.

    Each URL is represented as a tuple of the form
    ``uri = (url, queue, etag, mod_date, next_date, priority)``.

    The queue is ordered using the ``next_date`` in ascending fashion.
    """

    def __init__(self, db_name):
        """
        Initialize the simple uri queue.

        This is a single queue working only with one host!
        """
        SQLiteStore.__init__(self, db_name)

        # create the tables if they do not exist
        self._cursor.executescript("""
                CREATE TABLE IF NOT EXISTS queues(
                    url TEXT PRIMARY KEY ASC,
                    queue INTEGER,
                    etag TEXT,
                    mod_date INTEGER,
                    next_date INTEGER,
                    priority INTEGER
                );

                CREATE TABLE IF NOT EXISTS queue_identifiers(
                    queue INTEGER,
                    identifier TEXT,
                    PRIMARY KEY (queue, identifier)
                );

                CREATE INDEX IF NOT EXISTS queue_fifo ON queues(
                    queue,
                    next_date ASC
                );
                """)

    def add_uri(self, uri):
        """
        Add the uri to the given queue.
        """
        self._cursor.execute("""INSERT INTO queues
                (url, queue, etag, mod_date, next_date, priority) VALUES
                (?, ?, ?, ?, ?, ?)""", uri)

    def add_uris(self, uris):
        """
        Add the list of uris to the given queue.
        """
        self._cursor.executemany("""INSERT INTO queues
                (url, queue, etag, mod_date, next_date, priority) VALUES
                (?, ?,  ?, ?, ?, ?)""", uris)

    def update_uri(self, uri):
        """
        Update the uri.
        """
        (url, queue, etag, mod_date, next_date, prio) = uri
        self._cursor.execute("""UPDATE queues SET queue=?,
                etag=?, mod_date=?, next_date=?, priority=?
                WHERE url=?""", (queue, etag, mod_date, next_date, prio, url))

    def update_uris(self, uris):
        """
        Update the list of uris in the database.
        """
        update_uris = [(queue, etag, mod_date, next_date, priority, url)
            for (url, queue, etag, mod_date, next_date, priority) in uris]
        self._cursor.executemany("""UPDATE queues SET queue=?,
                etag=?, mod_date=?, next_date=?, priority=?
                WHERE url=?""", update_uris)

    def ignore_uri(self, url, status):
        """
        Called when an URI should be ignored. This is usually the case when
        there is a HTTP 404 or recurring HTTP 500's.
        """
        self.update_uri((url, None, None, None, status, 1))

    def queue_head(self, queue, n=1, offset=0):
        """
        Return the top `n` elements from the `queue`. By default, return the top
        element from the queue.

        If you specify `offset` the first `offset` entries are ignored.

        Any entries with a `next_date` below `1000` are being ignored. This
        enables the crawler to ignore URIs _and_ storing the status code.
        """
        self._cursor.execute("""SELECT * FROM queues
                WHERE queue = ?
                AND next_date > 1000
                ORDER BY next_date ASC
                LIMIT ?
                OFFSET ?""", (queue, n, offset))
        for row in self._cursor:
            yield (row['url'], row['queue'], row['etag'], row['mod_date'],
                row['next_date'], row['priority'])

    def remove_uris(self, uris):
        """
        Remove all uris.
        """
        del_uris = [(url,) for (url, _queue, _etag, _mod_date, _queue,
                _next_date) in uris]
        self._cursor.executemany("DELETE FROM queues WHERE url=?",
                del_uris)

    def qsize(self, queue=None):
        """
        Calculate the number of known uris.

        @param queue: if this is `None`, the size of all queues is returned.
        """
        if queue:
            cursor = self._cursor.execute("""SELECT count(url) FROM queues
                    WHERE queue=?""", (queue,))
        else:
            cursor = self._cursor.execute("""SELECT count(url) FROM queues""")
        return cursor.fetchone()[0]

    def all_uris(self, queue=None):
        """
        A generator for iterating over all available urls.

        Note: does not return the full uri object, only the url. This will be
        used to refill the unique uri filter upon restart.

        @param queue: int of the queue to use. If `None`, all URLs will be
            returned.
        @return: URL as str
        """
        if queue:
            self._cursor.execute("""SELECT url FROM queues WHERE queue=?""",
                    queue)
        else:
            self._cursor.execute("""SELECT url FROM queues""")
        for row in self._cursor:
            yield row['url']

    def get_uri(self, url):
        """
        Return the *URI* tuple for the given ``URL``.

        @return: (url, queue, etag, mod_date, next_date, priority)
        """
        self._cursor.execute("SELECT * FROM queues WHERE url=?",
                (url,))
        row = self._cursor.fetchone()
        if row:
            return (row['url'], row['queue'], row['etag'], row['mod_date'],
                    row['next_date'], row['priority'])
        raise UriNotFound(url)

    def get_all_queues(self):
        """
        A generator for iterating over all available queues.

        @return: (queue, identifier) as (int, str)
        """
        self._cursor.execute("SELECT * FROM queue_identifiers")
        for row in self._cursor:
            yield (row['queue'], row['identifier'])

    def get_queue_for_ident(self, identifier):
        """
        Get the ``queue`` for the given identifier if there is one.
        Raises a `QueueNotFound` error if there is no queue with the
        identifier.

        @param identifier: str of the identifier
        @return: the queue's id as int
        """
        self._cursor.execute("""SELECT queue FROM queue_identifiers WHERE
                identifier=?""", (identifier,))

        row = self._cursor.fetchone()
        if row:
            return row['queue']
        raise QueueNotFound(identifier)

    def add_or_create_queue(self, identifier):
        """
        Add a new queue with the ``identifier``. If the queue already exists,
        it's id is returned.

        @return: The new queue's id
        """
        try:
            return self.get_queue_for_ident(identifier)
        except QueueNotFound:
            pass

        self._cursor.execute("SELECT MAX(queue) AS id FROM queue_identifiers")
        row = self._cursor.fetchone()

        if row['id']:
            next_id = row['id'] + 1
        else:
            next_id = 1

        self._cursor.execute("INSERT INTO queue_identifiers VALUES(?,?)",
            (next_id, identifier))
        return next_id
