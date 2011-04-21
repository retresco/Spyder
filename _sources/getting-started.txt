.. vim: set fileencoding=UTF-8 :
.. vim: set tw=80 :
.. include:: globals.rst

.. _secgettingstarted:

Getting Started
===============

*Spyder* is just a library for creating web crawlers. In order to really crawl
content, you first have to create a *Spyder* skeleton:

   $ mkdir my-crawler && cd my-crawler
   $ spyder start
   $ ls
   log logging.conf master.py settings.py sink.py spyder-ctrl.py

This will copy the skeleton into `my-crawler`. The main file is `settings.py`.
In it, you can configure the logging level for **Masters** and **Workers** and
define the **crawl scope**. In `master.py` you should manipulate the starting
URLs and add your specific `sink.py` into the **Frontier**. `spyder-ctrl.py` is
just a small control script that helps you start the **Log Sink**, **Master** and
**Worker**.

In the skeleton everything is setup as if you would want to crawl Sailing
related pages from **DMOZ**. That should give you a starting point for your own
crawler.

So, when you wrote your sink and have everything configured right, it's time to
start crawling. First, on one of your nodes you start the logsink:

   $ spyder-ctrl.py logsink &

Again on one node (the same as the logsink, e.g.) you start the **Master**:

   $ spyder-ctrl.py master &

Finally you can start as many **Workers** as you want:

   $ spyder-ctrl.py worker &
   $ spyder-ctrl.py worker &
   $ spyder-ctrl.py worker &

Here we started 3 workers since it is a powerful node having a quad core CPU.


Scaling the Crawl
=================

With the default settings it is not possible to start workers on different
nodes. Most of the time one node is powerful enough to crawl quite an amount of
data. But there are times when you simply want to crawl using *many* nodes. This
can be done by configuring the **ZeroMQ** transports to something like

   
    ZEROMQ_MASTER_PUSH = "tcp://NodeA:5005"
    ZEROMQ_MASTER_SUB = "tcp://NodeA:5007"

    ZEROMQ_MGMT_MASTER = "tcp://NodeA:5008"
    ZEROMQ_MGMT_WORKER = "tcp://NodeA:5009"

    ZEROMQ_LOGGING = "tcp://NodeA:5010"

Basically we have setup a 2 node crawl cluster. **NodeA** acts as logging sink
and controls the crawl via the **Master**. **NodeB** Is a pure **Worker** node.
Only the **Master** actually *binds* **ZeroMQ** sockets, the **Worker** always
*connect* to them so the **Master** does not have to know where the
**Workers** are really running.


From here
=========

There is plenty of room for improvement and development ahead. Everything will
be handled by Github tickets from now on and, if there is interest, we may setup
a Google Group.
