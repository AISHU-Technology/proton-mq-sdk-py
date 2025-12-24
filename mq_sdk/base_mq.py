#!/usr/bin/env python
# -*- coding:utf-8 -*-

import asyncio
from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, Optional
import threading


class BaseMQ(ABC):
  """Base class for message queue implementations"""

  def __init__(self, mq_host: str, mq_port: int):
    self.mq_host = mq_host
    self.mq_port = mq_port
    self._resources_lock = threading.Lock()
    self._producers: Dict[str, Any] = {}
    self._consumers: Dict[str, Any] = {}

  @abstractmethod
  async def pub(self, topic: str, message: str) -> str:
    """Publish a message to a topic"""
    pass

  @abstractmethod
  async def sub(self, topic: str, group: str, handler: Callable[[str], None], **kwargs) -> None:
    """Subscribe to a topic"""
    pass

  def _acquire_resource(self, resource_dict: Dict[str, Any], key: str) -> Optional[Any]:
    """Thread-safe resource acquisition"""
    with self._resources_lock:
      return resource_dict.get(key)

  def _store_resource(self, resource_dict: Dict[str, Any], key: str, value: Any) -> None:
    """Thread-safe resource storage"""
    with self._resources_lock:
      resource_dict[key] = value

  def _remove_resource(self, resource_dict: Dict[str, Any], key: str) -> None:
    """Thread-safe resource removal"""
    with self._resources_lock:
      if key in resource_dict:
        del resource_dict[key]

  @abstractmethod
  def _cleanup_resource(self, resource: Any) -> None:
    """Clean up a specific resource"""
    pass

  def __del__(self):
    """Cleanup all resources on deletion"""
    with self._resources_lock:
      for resource in self._producers.values():
        self._cleanup_resource(resource)
      for resource in self._consumers.values():
        self._cleanup_resource(resource)
