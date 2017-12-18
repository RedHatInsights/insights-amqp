import json
import logging
import os
import pika
import shutil
import signal
import sys
import traceback
from insights.core import plugins
from insights.core import archives, specs
from insights.core.evaluators import InsightsEvaluator, SingleEvaluator, InsightsMultiEvaluator
from . import util, s3

WORK_QUEUE = os.environ.get("WORK_QUEUE", "engine_work")
MQ_URL = os.environ.get("MQ_URL", "amqp://localhost")
ARCHIVE_SOURCE = os.environ.get("ARCHIVE_SOURCE", "message")


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
        req_id = properties.headers.get("X-Request-Id")
        account = properties.headers.get("X-Account")
        setattr(util.thread_context, "request_id", req_id)

        if ARCHIVE_SOURCE == "message":
            buffer_ = body
        else:
            request = json.loads(body)
            buffer_ = s3.fetch(request["key"])

        extractor = archives.TarExtractor().from_buffer(buffer_)
        response = handle(extractor)
        s3.save(buffer_, response["system"].get("system_id"), extractor.content_type, account)
        shutil.rmtree(extractor.tmp_dir)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        ch.basic_publish(exchange="",
                         routing_key=properties.reply_to,
                         properties=pika.BasicProperties(correlation_id=properties.correlation_id,
                                                         content_type="application/json"),
                         body=json.dumps({"success": True, "response": response}))
    except KeyboardInterrupt:
        ch.basic_ack(delivery_tag=method.delivery_tag)
        ch.close()
    except:
        ch.basic_ack(delivery_tag=method.delivery_tag)
        ch.basic_publish(exchange="",
                         routing_key=properties.reply_to,
                         properties=pika.BasicProperties(correlation_id=properties.correlation_id,
                                                         content_type="application/json"),
                         body=json.dumps({"success": False, "reason": traceback.format_exc()}))
        logging.root.exception("Processing failed")
    else:
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


def health_check(signum, frame):
    with open("/tmp/insights-health", "w") as fp:
        fp.write("0")


if __name__ == "__main__":
    util.initialize_logging()
    for pkg in get_plugin_packages():
        logging.root.info("Loading %s", pkg)
        plugins.load(pkg)

    connection = pika.BlockingConnection(pika.URLParameters(MQ_URL))
    channel = connection.channel()
    channel.basic_qos(prefetch_count=1)
    channel.queue_declare(queue=WORK_QUEUE)
    channel.basic_consume(worker, queue=WORK_QUEUE)

    signal.signal(20, health_check)

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        pass
