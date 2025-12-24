from setuptools import find_packages, setup

setup(
  name="mq_sdk",  # Package name
  version="1.8.0",  # Package version
  description="Python SDK for NSQ and Kafka message queues",  # Brief description
  packages=find_packages(),  # Packages
  install_requires=[
    "tornado>=6.0.2",
    "pynsq==0.9.0",
    "confluent-kafka>=2.0.2",
    "ruamel.yaml==0.17.40",
    "IPy>=1.1"],  # Required dependencies
  zip_safe=(False),
)
