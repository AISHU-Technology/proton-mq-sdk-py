#!/usr/bin/env python3
# -*- coding:utf-8 -*-

import copy
import IPy
import asyncio

# import time
from types import FunctionType
from mq_sdk import http_client, logger
from confluent_kafka import Producer, Consumer


class KAFKAMoudle:
  MODULE_NAME = "KAFKAMoudle"
  LOGGER = logger.get_logger(MODULE_NAME)
  producer = None

  def __init__(self, kafka_host, kafka_port, **kwargs):
    hosts = [h.strip() for h in kafka_host.split(",")]
    bootstrap_servers = ",".join([f"[{h}]:{kafka_port}" if self.checkip(h) == 6 else f"{h}:{kafka_port}" for h in hosts])

    self.conf = {"bootstrap.servers": bootstrap_servers}
    if "username" in kwargs and "passwd" in kwargs:
      sasl_mechanisms = "SCRAM-SHA-512"
      if "sasl_mechanisms" in kwargs and kwargs["sasl_mechanisms"]:
        sasl_mechanisms = kwargs["sasl_mechanisms"]
      self.conf = {
        "bootstrap.servers": bootstrap_servers,
        "security.protocol": "SASL_PLAINTEXT",
        "sasl.mechanisms": sasl_mechanisms,
        "sasl.username": kwargs["username"],
        "sasl.password": kwargs["passwd"],
      }
    if "acks" in kwargs:
      self.conf.update({"acks": kwargs["acks"]})
    if KAFKAMoudle.producer is None:
      KAFKAMoudle.producer = Producer(**self.conf)

  @http_client.typeassert("", str, str)
  async def pub(self, topic, message):
    """Kafka producer"""

    def acked(err, msg):
      if err is not None:
        self.LOGGER.error("Failed to deliver message: {}".format(err))
        raise Exception("Failed to deliver message: {}".format(err))

    while True:
      try:
        KAFKAMoudle.producer.produce(topic, message.encode("utf-8"), callback=acked)
        KAFKAMoudle.producer.poll(0)
        break
      except BufferError:
        self.LOGGER.error(
          "Local producer queue is full ({} messages awaiting delivery): try again later, or increase queue size".format(len(KAFKAMoudle.producer))
        )
        KAFKAMoudle.producer.poll(0.1)
    KAFKAMoudle.producer.flush()

  @http_client.typeassert("", str, str, FunctionType, int, int)
  async def sub(self, topic, channel, message_handler, lookupd_poll_interval=60, max_in_flight=1):
    """Kafka consumer"""

    conf = copy.copy(self.conf)
    conf["group.id"] = channel
    conf["enable.auto.commit"] = False

    consumer = Consumer(**conf)
    consumer.subscribe([topic])
    done_msg = None
    # Manual offset commit is used for consumption
    while True:
      # msg = consumer.poll(timeout=1)
      msg = consumer.poll(timeout=0)
      if msg is None:
        await asyncio.sleep(0.1)
        continue
      elif msg.error():
        self.LOGGER.error("Error occured: {}".format(msg.error()))
        # 待定
        # time.sleep(10)
      else:
        try:
          if asyncio.iscoroutinefunction(message_handler):
            await message_handler(msg.value().decode("utf-8"))
          else:
            message_handler(msg.value().decode("utf-8"))
          done_msg = msg
          consumer.commit(message=done_msg, asynchronous=True)
        except Exception as e:
          self.LOGGER.error(str(e))

  def checkip(self, address):
    try:
      version = IPy.IP(address).version()
      if version == 4 or version == 6:
        return version
      else:
        return False
    except Exception as e:
      return False
