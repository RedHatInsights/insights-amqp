import os
import sys
import pika

WORK_QUEUE = os.environ.get("WORK_QUEUE", "engine_work")
MQ_HOST = os.environ.get("MQ_HOST", "localhost")


if len(sys.argv) < 2:
    print "Please specify archive path to process"
    sys.exit(1)
elif not os.path.isfile(sys.argv[1]):
    print "Please specify a valid archive path"

with open(sys.argv[1], "rb") as fp:
    archive = fp.read()

connection = pika.BlockingConnection(pika.ConnectionParameters(MQ_HOST))
channel = connection.channel()
channel.queue_declare(queue=WORK_QUEUE)

channel.basic_publish(exchange="", routing_key=WORK_QUEUE, body=archive)
channel.close()
