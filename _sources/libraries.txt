.. vim: set fileencoding=UTF-8 :
.. vim: set tw=80 :
.. include:: globals.rst

Libraries used in |spyder|
==========================

.. _seczmq:

ZeroMQ
------

Not only with the emergence of multicore systems Python's `Global Interpreter
Lock <http://www.python.org/NEEDSLINK>`_ becomes a major issue for scaling
across cores. Libraries like `multiprocess <http://NEEDSLINK>`_ try to
circumvent the `GIL` by forking child processes and establishing a messaging
layer between them. This enables Python programmers to scale with the number of
available cores but scaling across node boundaries is not possible using plain
`multiprocess`.

At this point `ZeroMQ <http://www.zeromq.org>`_ comes to the rescue. As the name
suggests, |zmq| is a message queue. But, unlike other more famous queues like
`AMQP` or more lightweight ones like `STOMP` or `XMPP`, |zmq| does not need a
global broker (that might act as *single point of failure*). It is instead a
little bit of code around the plain *socket* interface that adds simple
messaging patterns to them (it's like *sockets on steroids*).

The beauty of |zmq| lies in it's simplicity. The programmer basically defines
a *socket* to which one side **binds** and the other **connects** and a
messaging pattern with which both sides communicate with each other. Once this
is established, scaling across cores/nodes/data centers is simple as pie. Four
types of *sockets* are supported by |zmq|:

1. `inproc` sockets can be used for **intra-process** communication (between
    threads, e.g.)

2. `ipc` sockets can be used for **inter-process** communication between
    different processes *on the same node*.

3. `tcp` sockets can be used for **inter-process** communication between
    different processes *on different node*.

4. `pgn` sockets can be used for **inter-process** communication between one and
    many other processes *on many other nodes*.

So by simply changing the socket type from `ipc` to `tcp` the application can
scale across node boundaries transparently for the programmer, i.e. by **not
changing a single line of code**. Awesome!

This leaves us with the different messaging patterns. |zmq| supports all well
known (at least to me) messaging patterns. The first one that comes into mind is
of course the `PUB/SUB` pattern that allows one publisher to send messages to
many subscribers. The `PUSH/PULL` pattern allows one master to send messages to
only one of the available clients (the common producer/consumer pattern). With
`REQ/REP` a simple request and response pattern is possible. Most of the
patterns have a `non-blocking` equivalent.


Messaging Patterns used in |spyder|
+++++++++++++++++++++++++++++++++++

|zmq| is used as messaging layer to distribute the workload to an arbitrary
number of worker processes which in return send the result back to the master.
In the context of |spyder| the master process controls the |urls| that should be
crawled and sends them to the worker processes when they are due. One of the
worker processes then downloads the content and possibly extracts new links from
it. When finished it sends the result back to the master.

We do not use the `REQ/REP` pattern as it does not scale as easily as we need
since we have to keep track of whom we sent the |url| to and we would have to do
the load balancing ourselves.

Instead with the |pushpull| pattern we get the load balancing as a nice little
gift. It comes with a *fair distribution policy* that simply distributes the
messages to all workers in a *round-robin* way. In order to send the results
back to the master we will use the |pubsub| pattern where the *publisher* is the
worker process and the *subscriber* is the master process.

The |pubsub| pattern is used to send the results back to the master process.

Users familiar with |zmq| might already have noted that this messaging setup is
shamelessly *adapted* from `Mongrel2 <http://www.mongrel.org>`_. In the case of
a *Web Server* as well as for a crawler this is a perfect fit as it helps you to
scale **very** easy.

.. note:: There is another way to do this type message pattern using
  *XPEQ/XREP*. Transition to this pattern is planned for the near future.

For a crawler there are two parts that we possibly want to scale: the worker
*and* the master. While scaling the worker across several processes is somewhat
obvious, scaling the master first seems to be of no relevance. But if you want
to crawl large portions of the web (all German Internet pages, e.g.), you might
experience difficulties as this are not only **many** |urls| but also **many**
hosts you possibly want to connect. While the number of |urls| might not be the
limiting part, the number of hosts can be as they require a lot of queue
switching.

.. note:: For more info on this, see the :ref:`seccrawlerdesign` document.


What does all that mean in practice
+++++++++++++++++++++++++++++++++++

The master process binds to one socket with a `PUSH` type and to another socket
using the `SUB` type. On the `SUB` socket the master registers a |zmq| filter to
only receive messages with a certain *topic*: it's identity.

The worker in connects to the `PUSH` socket using a `PULL` type socket and
receives the |urls| from the master containing the master's identity. When the
|url| has been processed it sends the result back to the master using the `PUB`
socket it has connected to the `SUB` socket. By setting the message's topic to
the identity of the sending master, it is ensured that only the master process
that sent this |url| receives the answer.

Future version of |spyder| will thus be able to work with **n** master and **m**
worker processes.


.. _sectornado:

|tornado|
---------

`Tornado <http://github.com/facebook/tornado>`_ is a *non-blocking* or *evented
IO* library developed at FriendFeed (now Facebook) to run their python front-end
servers.  Basically this is a

.. code-block:: python

    while True:
        callback_for_event(event)

loop. The events are any *read* or *write* event on a number of sockets or files
that are registered with the loop. So instead of starting one thread for each
socket connection everything runs in one thread or even process. Although this
might feel strange it has been shown to be **alot** faster for network intensive
applications that potentially serve a large number of clients.

.. note:: For more info see the `C10k Problem <http://NEEDS-A-LINK>`_


An additional reason for choosing |tornado| was the nice integration with |zmq|.
This not only makes programming with |zmq| easier but also makes it possible to
easily write *non-blocking, evented* IO programms with Python and |zmq|.
