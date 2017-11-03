import json
import logging
import os
import pika
import sys
import traceback
from insights.core import plugins
from insights.core import archives, specs
from insights.core.evaluators import InsightsEvaluator, SingleEvaluator, InsightsMultiEvaluator

WORK_QUEUE = os.environ.get("WORK_QUEUE", "engine_work")
RETURN_QUEUE = os.environ.get("RETURN_QUEUE", "engine_return")
MQ_HOST = os.environ.get("MQ_HOST", "localhost")


def handle(extractor, system_id=None, account=None, config=None):
    spec_mapper = specs.SpecMapper(extractor)

    md_str = spec_mapper.get_content("metadata.json", split=False, default="{}")
    md = json.loads(md_str)

    if md and 'systems' in md:
        runner = InsightsMultiEvaluator(spec_mapper, system_id, md)
    elif spec_mapper.get_content("machine-id"):
        runner = InsightsEvaluator(spec_mapper, system_id=system_id)
    else:
        runner = SingleEvaluator(spec_mapper)
    return runner.process()


def worker(ch, method, properties, body):
    try:
        extractor = archives.TarExtractor().from_buffer(body)
        response = handle(extractor)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        ch.basic_publish(exchange="",
                         routing_key=RETURN_QUEUE,
                         properties=pika.BasicProperties(content_type="application/json"),
                         body=json.dumps(response, indent=4))
    except KeyboardInterrupt:
        ch.basic_ack(delivery_tag=method.delivery_tag)
        ch.close()
    except:
        ch.basic_ack(delivery_tag=method.delivery_tag)
        ch.basic_publish(exchange="",
                         routing_key=RETURN_QUEUE,
                         properties=pika.BasicProperties(content_type="text/plain"),
                         body=traceback.format_exc())
    logging.root.info("Processed archive")


def get_plugin_packages():
    if len(sys.argv) > 1:
        return sys.argv[1:]
    elif "RULE_PACKAGES" in os.environ:
        return os.environ["RULE_PACKAGES"].split(",")
    else:
        print "No plugin packages found."
        print "Specify packages via cmdline argumens or via RULE_PACKAGES environment variable"
        sys.exit(1)


if __name__ == "__main__":
    logging.basicConfig()
    logging.root.setLevel(logging.INFO)
    for pkg in get_plugin_packages():
        print "Loading %s" % pkg
        plugins.load(pkg)
    connection = pika.BlockingConnection(pika.ConnectionParameters(MQ_HOST))
    channel = connection.channel()
    channel.basic_qos(prefetch_count=1)
    channel.queue_declare(queue=WORK_QUEUE)
    channel.queue_declare(queue=RETURN_QUEUE)
    channel.basic_consume(worker, queue=WORK_QUEUE)
    channel.start_consuming()
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        pass
