import json
import os
import sys
import pika
import uuid
from . import s3

WORK_QUEUE = os.environ.get("WORK_QUEUE", "engine_work")
RETURN_QUEUE = os.environ.get("RETURN_QUEUE", "engine_return")
MQ_HOST = os.environ.get("MQ_HOST", "localhost")
ARCHIVE_SOURCE = os.environ.get("ARCHIVE_SOURCE", "message")


if len(sys.argv) < 2:
    print "Please specify archive path to process"
    sys.exit(1)
elif not os.path.isfile(sys.argv[1]):
    print "Please specify a valid archive path"

if ARCHIVE_SOURCE == "message":
    with open(sys.argv[1], "rb") as fp:
        archive = fp.read()
else:
    archive_id = str(uuid.uuid4())
    s3.s3_post(sys.argv[1], s3.transfer_bucket, archive_id, None)
    archive = json.dumps({"key": archive_id})


def handle_response(ch, method, properties, body):
    response = json.loads(body)
    print json.dumps(response, indent=4)
    print "Success!" if response["success"] else "Failure!"
    ch.basic_ack(delivery_tag=method.delivery_tag)


def handle_one_response(ch, method, properties, body):
    handle_response(ch, method, properties, body)
    ch.close()


connection = pika.BlockingConnection(pika.ConnectionParameters(MQ_HOST))
channel = connection.channel()
channel.queue_declare(queue=WORK_QUEUE)
channel.queue_declare(queue=RETURN_QUEUE)

print "Posting archive"
channel.basic_publish(exchange="", routing_key=WORK_QUEUE, body=archive,
                      properties=pika.BasicProperties(reply_to=RETURN_QUEUE))
channel.basic_consume(handle_one_response, queue=RETURN_QUEUE)
channel.start_consuming()
