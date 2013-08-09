pyAPS
=====

Simple example showing how to use Mongo expiring records to make sure a task only runs on one process.

Requires pyMongo >= 2.4, and a locally running version of MongoDB at 127.0.0.1:27017.  (I used MongoDB 2.4.5)

-----
To see it in action, run it from the command line in two or three terminals at the same time.

As MongoDB removes expiring records on TTL collections every 60 seconds, tasks can only be reliably scheduled if they're
120 seconds or more apart.  The seven second interval in this example is to simply show output that the tasks are doing
something, even if that something is just sleeping.

Tasks are send off to be run in another process when they're executed as well, so they don't block the main process.