.. vim: set fileencoding=UTF-8 :
.. vim: set tw=80 :
.. include:: globals.rst

Roadmap
=======

Version 0.3
+++++++++++

- Integration with `Supervisord`

    The current way of starting |spyder| is quite painful. Using the
    `supervisord` I want to start the master and worker processes automatically
    and, in case of failures, be able to restart them automatically.
