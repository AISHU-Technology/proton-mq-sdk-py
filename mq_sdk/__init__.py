from . import nsq_moudle, kafka_module, http_client, logger
import os

T9CLILOGDIR = os.getenv("T9CLILOGDIR")
if not T9CLILOGDIR:
  os.environ["T9CLILOGDIR"] = "/tmp"
