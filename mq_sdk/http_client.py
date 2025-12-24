#!/usr/bin/env python
# -*- coding:utf-8 -*-
import json
import logging
from inspect import signature
from urllib import parse

from tornado.httpclient import AsyncHTTPClient

parse_host = lambda h: f"[{h}]" if ":" in h else h  # NOQA: E999


class TimeoutErr(Exception):
  """
  Timeout error
  """

  def __init__(self, cause: str = None):
    super().__init__()
    self.cause = cause

  def __str__(self) -> str:
    return self.cause or "Timeout Linked"


class SocketErr(TimeoutErr):
  """
  Socket error
  """


logger = logging.getLogger("http_client")  # pylint: disable=invalid-name


def __expand_query(old, query):
  qsl = parse.parse_qsl(old)
  for name, val in query.items():
    qsl.append((name, val))
  return parse.urlencode(qsl)


def __expand_querys(old, querys):
  qsl = parse.parse_qsl(old)
  for name, vals in querys.items():
    for val in vals:
      qsl.append((name, val))
  return parse.urlencode(qsl)


def specific_host(
  host: str,
  port: str,
  protocol: str,
) -> str:
  """
  根据 协议 host port 返回URL
  """
  hosts = host.split(",")
  if len(hosts) > 1:
    for i in range(len(hosts)):
      hosts[i] = f"{protocol}://{parse_host(hosts[i])}:{port}"
      host = ",".join(hosts)
    return f"({host})"
  return f"{protocol}://{parse_host(host)}:{port}"


async def fetch(url, method, body=None, **kwargs):
  """发送http请求"""
  if "headers" not in kwargs:
    kwargs["headers"] = {
      "Content-Type": "application/json",
      # 'Authorization': "Bearer " + OAuth.selftoken
    }
  else:
    if "Content-Type" not in kwargs["headers"]:
      kwargs["headers"]["Content-Type"] = "application/json"

    # if 'Authorization' not in kwargs['headers']:
    #     kwargs['headers']['Authorization'] = "Bearer " + OAuth.selftoken

  if "validate_cert" not in kwargs:
    kwargs["validate_cert"] = False

  # 增加query string支持
  if "query" in kwargs:
    query = kwargs["query"]
    assert isinstance(query, dict)
    del kwargs["query"]

    splited = list(parse.urlsplit(url))
    splited[3] = __expand_query(splited[3], query)
    url = parse.urlunsplit(splited)

  # 增加querys arrey支持
  if "querys" in kwargs:
    querys = kwargs["querys"]
    assert isinstance(querys, dict)
    del kwargs["querys"]

    splited = list(parse.urlsplit(url))
    splited[3] = __expand_querys(splited[3], querys)
    url = parse.urlunsplit(splited)

  if isinstance(body, (dict, list)):
    body = json.dumps(body)

  logger.debug('Fetch "%s". method: %s, body: %s, kwargs: %s.', url, method, body, kwargs)
  http_client = AsyncHTTPClient()

  return await http_client.fetch(url, method=method, body=body, raise_error=False, **kwargs)


async def post(url, body=None, **kwargs):
  return await fetch(url, "POST", body=body, **kwargs)


async def get(url, **kwargs):
  return await fetch(url, "GET", **kwargs)


async def put(url, body=None, **kwargs):
  return await fetch(url, "PUT", body=body, **kwargs)


async def delete(url, body=None, **kwargs):
  return await fetch(url, "DELETE", body=body, **kwargs)


def typeassert(*args, **kwargs):
  def decorator(fun):
    sig = signature(fun)
    btypes = sig.bind_partial(*args, **kwargs).arguments

    def wrapper(*funargs, **funkwargs):
      for name, stype in sig.bind_partial(*funargs, **funkwargs).arguments.items():
        if name == "self":
          continue
        if name in btypes:
          if not isinstance(stype, btypes[name]):
            raise TypeError("'%s' must be '%s'" % (name, btypes[name]))
      return fun(*funargs, **funkwargs)

    return wrapper

  return decorator
