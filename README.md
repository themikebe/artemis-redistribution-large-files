# artemis-redistribution-large-files

this project is an attempt to adapt "queue-redistibution" example from the apache-artemis sources
to make it work with large files.


My observation is that the (large) messages get stuck on internal queues instead of being redistributed. 