.. vim: set fileencoding=UTF-8 :
.. vim: set tw=80 :
.. include:: globals.rst
.. _secrelnotes:

Release Notes
=============

Version 0.1
-----------

This is the first release of the |spyder| so I will only cover the known issues
here.

Changes
+++++++

* Initial Release with a working *master* and *worker* implementation

Known Issues
++++++++++++

* If a *worker* crashes or is being stopped, the URLs it is currently processing
  might get lost in the *master* and never be crawled. There are several
  precautions in order to track this problem in the future but right now it is a
  bug that might also end up in a memory leak.
