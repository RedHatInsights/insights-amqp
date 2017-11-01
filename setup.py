import os
from setuptools import setup, find_packages

package_info = {k: None for k in ["RELEASE", "COMMIT", "VERSION"]}

for name in package_info:
    with open(os.path.join("insights_amqp", name)) as f:
        package_info[name] = f.read().strip()


if __name__ == "__main__":
    setup(
        name="insights-amqp",
        version=package_info["VERSION"],
        description="Process Insights archives over AMQP",
        keywords=['insights', 'redhat'],
        packages=find_packages(),
        install_requires=['insights-core', 'pika'],
    )
