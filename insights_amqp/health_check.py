import os
import sys
import time

HEALTH_FILE = "/tmp/insights-health"

if os.path.exists(HEALTH_FILE):
    os.remove(HEALTH_FILE)

# os.kill(1, 20)

while True:
    if os.path.exists(HEALTH_FILE):
        with open(HEALTH_FILE) as fp:
            sys.exit(int(fp.read()))
    time.sleep(.25)
