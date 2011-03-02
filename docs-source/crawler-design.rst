.. vim: set fileencoding=UTF-8 :
.. vim: set tw=80 :
.. include:: globals.rst
.. _seccrawlerdesign:

Crawler Design
==============

The basic crawler design is simple and straight forward. You have a *Master*
that collects the |urls| that should be crawled and a number of *Worker* threads
(or processes) that download the content and extract new links from it. In
practice though there are a number of pitfalls you have to keep an eye on. Just
to give one example: you really don't want to excessively crawl **one** host as
you might be doing a *Denial of Service* attack given enough workers. And even
if the host survives, the site owner might not like you from now on.

Some Science
------------

Ok, really only a little bit. Basically there two papers describing effective
crawler designs. The *Mercator* paper (**LINK NEEDED**) describes the
architecture of the *Mercator* crawler. The crawler is split into several parts:

* *Frontier* for keeping track of |urls|
* *Scheduler* for scheduling the |urls| to be crawled
* *Downloader* for really downloading the content
* *Link Extractors* for extracting new links from different kinds of content
* *Unique Filter* for filtering known |urls| from the extracted ones
* *Host Splitter* for working with multiple *Frontiers*

The second important paper on crawler design is the *Ubi Crawler* (**LINK
NEEDED**). In this paper the authors use a *Consistent Hashing* algorithm for
splitting the hosts among several *Frontiers*.

The |spyder| is designed on the basis of these two papers.

References
==========

The |spyder| is not only inspired by these two papers but also on `Heritrix
<http://crawler.archive.org>`_ the *Internet Archive's* open source crawler.
*Heritrix* is designed just like *Mercator* except it lacks something like a
*Host Splitter* that allows one to crawl using more than one node. Additionally
*Heritrix* does not provide any kind of *monitoring* or *revisiting* strategy,
although this might be possible in Version *H3*.
