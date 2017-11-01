# Insights AMQP

Process insights archives over a message queue using AMQP via the pika lib.
Currently tested against RabbitMQ running locally and in Openshift.

Currently there is a client and a worker set up to use a single worker queue.
The client will post a message with an archive (.tar.gz) as the body, and the
worker will consume all the messages on the queue.

To start up a worker:

    python insights_amqp/worker.py <package> [<package>, [<package>] ...]

To post an archive:

    python insights_amqp/client.py <archive path>
