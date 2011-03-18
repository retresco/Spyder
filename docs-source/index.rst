.. vim: set fileencoding=UTF-8 :
.. vim: set tw=80 :
.. include:: globals.rst

Welcome to |spyder|
===================

|spyder| is a scalable web-spider written in Python using the non-blocking
|tornado| library and |zmq| as messaging layer. The messages are serialized
using *Thrift*.

The architecture is very basic: a **Master** process contains the crawl
**Frontier** that organises the |urls| that need to be crawled; several
**Worker** processes actually download the content and extract new |urls| that
should be crawled in the future. For storing the content you may attach a
**Sink** to the **Master** and be informed about the interesting events for an
|url|.

Table of Contents
=================

.. toctree::
   :maxdepth: 2

   release-notes
   getting-started
   crawler-design
   libraries
   api/spyderapi
   roadmap

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

