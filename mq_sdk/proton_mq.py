#!/usr/bin/env python
# -*- coding:utf-8 -*-
import socket

from ruamel import yaml
from mq_sdk import http_client, logger, kafka_module, nsq_moudle


class Connector:
  MODULE_NAME = "Proton-MQ"
  LOGGER = logger.get_logger(MODULE_NAME)

  @staticmethod
  # @http_client.typeassert(tuple, list)
  # @http_client.typeassert(str, int, str, int, str, tuple)
  # def get_connector(phost, pport, chost, cport, connector_type):
  def get_connector(phost, pport, chost, cport, connector_type, **kwargs):
    mq_host = phost
    mq_port = pport

    # phosts=phost.split(",")
    # chosts=phost.split(",")
    # if len(phosts)>1:
    #     for i in range (len(phosts)):
    #         Connector.ping(phosts[i],pport)
    # elif len(phosts) == 1:
    #     Connector.ping(phost,pport)
    # Only nsq and kafka are supported, other types are not supported
    if connector_type == "nsq":
      # nsq requires validation of both producer and lookupd
      Connector.ping(mq_host, mq_port)
      Connector.ping(chost, cport)
    elif connector_type == "kafka":
      Connector.ping(mq_host, mq_port)
    else:
      msg = f"Unsupported Message Queuing Middleware type: {connector_type}. Only 'nsq' and 'kafka' are supported."
      Connector.LOGGER.error(msg)
      raise Exception(msg)

    if connector_type == "nsq":
      return nsq_moudle.NSQMoudle(mq_host, mq_port, chost, cport)
    elif connector_type == "kafka":
      return kafka_module.KAFKAMoudle(mq_host, mq_port, **kwargs)

  @staticmethod
  @http_client.typeassert(str)
  def get_connector_from_file(configFile):
    with open(configFile, "rb") as f:
      yamlMsg = yaml.load(f.read(), Loader=yaml.Loader)
    auth_info = {}
    if "auth" in yamlMsg and yamlMsg["auth"]:
      auth_info = {"username": yamlMsg["auth"]["username"], "passwd": yamlMsg["auth"]["password"], "sasl_mechanisms": yamlMsg["auth"]["mechanism"]}
    return Connector.get_connector(
      yamlMsg["mqHost"], yamlMsg["mqPort"], yamlMsg["mqLookupdHost"], yamlMsg["mqLookupdPort"], yamlMsg["mqType"], **auth_info
    )

  @staticmethod
  def ping(host, port):
    sk = None
    try:
      sk = socket.create_connection(address=(host, port), timeout=1)
    except Exception:
      raise Exception("Server address not available:" + host + ":" + str(port))
    finally:
      if sk:
        sk.close
