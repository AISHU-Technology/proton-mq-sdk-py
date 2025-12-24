#!/usr/bin/env python
# -*- coding:utf-8 -*-
import asyncio
from types import FunctionType

import nsq

from mq_sdk import http_client, logger


class NSQMoudle:
  MODULE_NAME = "NSQMoudle"
  LOGGER = logger.get_logger(MODULE_NAME)

  def __init__(self, phost, pport, chost, cport):
    self.phost = phost
    self.pport = pport
    self.chost = chost
    self.cport = cport

  def _nsq_producer_url(self, topic) -> str:
    """Get NSQ producer URL"""
    return http_client.specific_host(self.phost, self.pport, "http") + "/pub?topic=" + topic

  def __nsq_consumer_url(self) -> str:
    """Get NSQ consumer configuration URL"""
    return http_client.specific_host(self.chost, self.cport, "http")

  @http_client.typeassert("", str, str)
  async def pub(self, topic, message):
    """NSQ producer"""
    url = self._nsq_producer_url(topic)
    ret = await http_client.post(url, body=message, headers={"Connection": "close"})
    # self.LOGGER.info("Send to %s:%s complete, topic: %s, msg:%s", self.phost, self.pport,topic, message)
    return ret.reason
    # try:
    #     await http_client.post(url, body=message,
    #                            headers={"Connection": "close"})
    #     self.LOGGER.info("Send to %s:%s complete, topic: %s, msg:%s", self.phost, self.pport,
    #                 topic, message)
    # except Exception as ex:  # pylint: disable=broad-except
    #     self.LOGGER.error("Send to %s:%s failed: %s, topic: %s, msg:%s", self.phost,
    #                  self.pport, str(ex), topic, message)

  @http_client.typeassert("", str, str, FunctionType, int, int)
  async def sub(self, topic, channel, message_handler, lookupd_poll_interval=60, max_in_flight=1):
    """nsq消费者"""
    url = self.__nsq_consumer_url()

    self.LOGGER.info("NSQLookupdAddress: %s", url)

    if lookupd_poll_interval > 1000:
      lookupd_poll_interval = 1000
      self.LOGGER.warn("lookupd_poll_interval the value out of range, reset to 1000")
    elif lookupd_poll_interval < 1:
      lookupd_poll_interval = 1
      self.LOGGER.warn("lookupd_poll_interval the value out of range, reset to 1")

    if max_in_flight > 16:
      max_in_flight = 16
      self.LOGGER.warn("max_in_flight the value out of range, reset to 16")
    elif max_in_flight < 1:
      max_in_flight = 1
      self.LOGGER.warn("max_in_flight the value out of range, reset to 1")
    createTopicUrl = f"http://{http_client.parse_host(self.phost)}:{self.pport}/topic/create?topic={topic}"
    createChannelUrl = url + "/channel/create?topic=" + topic + "&" + "channel=" + channel
    while True:
      try:
        await http_client.post(createTopicUrl, body={}, headers={"Connection": "close"})
        await http_client.post(createChannelUrl, body={}, headers={"Connection": "close"})
        break
      except Exception as ex:
        self.LOGGER.error("create topic or channel error: %s, topic: %s, channel: %s", str(ex), topic, channel)
        await asyncio.sleep(1)
    try:

      def process_handler(message):
        class CovertMessage(nsq.Message):
          def __str__(self):
            return self.body.decode()

        return message_handler(CovertMessage(message.id, message.body, message.timestamp, message.attempts))

      reader = nsq.Reader(
        message_handler=process_handler,
        lookupd_http_addresses=[url],
        topic=topic,
        channel=channel,
        max_in_flight=max_in_flight,
        lookupd_poll_interval=lookupd_poll_interval,
      )
      self.LOGGER.info("Connect to nsqlookupd success, topic: %s, channel: %s", topic, channel)
    except Exception as ex:  # pylint: disable=broad-except
      self.LOGGER.error("Connect to nsqlookupd error: %s, topic: %s, channel: %s", str(ex), topic, channel)
