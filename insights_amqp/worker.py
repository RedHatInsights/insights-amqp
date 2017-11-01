import json
import os
import pika
import sys
from insights.core import plugins
from insights.core import archives, specs
from insights.core.evaluators import InsightsEvaluator, SingleEvaluator, InsightsMultiEvaluator

WORK_QUEUE = os.environ.get("WORK_QUEUE", "engine_work")
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
        print json.dumps(response, indent=4)
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except KeyboardInterrupt:
        ch.basic_nack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print "Worker failure: %s" % e.message
        ch.basic_ack(delivery_tag=method.delivery_tag)


def get_plugin_packages():
    if len(sys.argv) > 1:
        return sys.argv[1:]
    elif "PLUGIN_PACKAGES" in os.environ:
        return os.environ["PLUGIN_PACKAGES"].split(",")
    else:
        print "No plugin packages found."
        print "Specify packages via cmdline argumens or via PLUGIN_PACKAGES environment variable"
        sys.exit(1)


if __name__ == "__main__":
    for pkg in get_plugin_packages():
        print "Loading %s" % pkg
        plugins.load(pkg)
    connection = pika.BlockingConnection(pika.ConnectionParameters(MQ_HOST))
    channel = connection.channel()
    channel.basic_qos(prefetch_count=1)
    channel.queue_declare(queue=WORK_QUEUE)
    channel.basic_consume(worker, queue=WORK_QUEUE)
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        pass
