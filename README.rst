Spyder
======

`ALONG CAME A SPIDER`


Spyder's communication
======================

Two different types of communication exist between the diffent Spyder
components. The management layer simply follows a publish subscribe pattern.
The `master` publishes messages and the `worker` processes react on them. In
order to increase fault detection, every `worker` sends an ACK message to the
publishing `master`.

Then there is the data layer. The `master` pushes data out to the `worker` in
order to get them processed. When the data has been processed, i.e. all content
has been saved and new URLs haven been extracted, the data is sent back to the
sending `master`.

Message Formats
==============

Management messages have the following format:

    Part 1    Key           A Subscription Key
    Part 2    Identity      The sender's identity
    Part 3    Data          The message data

For management messages `Data` will be some kind of instruction the `workers`
are supposed to do, e.g. stop working. The `Key` helps in sending messages only
to parts of the system, e.g. to all `workers`. The `Identity` is used to answer
the specific sender that the instruction has been completed successfully.

Data messages have the following format:

    Part 1    Identity      The sender's identity
    Part 2    Data          The messages Data

Here no `Key` is needed as there are only `workers` listening on that specific
socket. The `Data` is a `CrawlUri` that has been serialized using `Thrift`
(see: *crawluri.thrift*).
