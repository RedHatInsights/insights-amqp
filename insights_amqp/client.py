import os
import sys
import pika


if len(sys.argv) < 2:
    print "Please specify archive path to process"
    sys.exit(1)
elif not os.path.isfile(sys.argv[1]):
    print "Please specify a valid archive path"

with open(sys.argv[1], "rb") as fp:
    archive = fp.read()

connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
channel = connection.channel()
channel.queue_declare(queue="engine_work")

channel.basic_publish(exchange="", routing_key="engine_work", body=archive)
channel.close()
