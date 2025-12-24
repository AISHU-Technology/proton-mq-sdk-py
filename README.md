# proton-mq Python SDK

## 1. API Design

The SDK provides a `Connector` class with static methods to create connector instances.

    get_connector(mq_host: string, mq_port: int, mq_lookupd_host: string, mq_lookupd_port: int, connector_type: string) -> connector  

    # Connector instance method to publish a message, returns after completion
    pub(topic: str, msg: str)  

    # Connector instance method to consume messages, this function blocks after being called
    sub(topic: str, channel: str, handler: func(msg: str)->bool, lookupd_poll_interval: int, max_in_flight: int)  

## 2. Installation

    pip install dist/mq_sdk-1.0.0.tar.gz 

## 3. Usage Examples

    from mq_sdk.proton_mq import Connector  
    
    # Create connector via parameters (nsq)
    cnt = Connector.get_connector("localhost", "4151", "localhost", "4161", "nsq")  
    
    # Create connector from config file
    cnt = Connector.get_connector_from_file("config.yaml")
    
    # Create connector with IPv6 address (nsq)
    cnt = Connector.get_connector("::ffff:localhost", "4151", "::ffff:localhost", "4161", "nsq") 
     
    cnt.pub('topic66', "proton-hello")  # Publish message
    cnt.sub(topic, channel, handler, 1000, 99)  # Consume messages

Supported message queue types: nsq, kafka

For high concurrency with Python single-threaded execution, it is recommended to start a separate thread or process for the consumer. See examples in the `example` directory: `kafka_demon_consumer.py`, `demon_consumer_thread.py`.

For more details, see the `example` directory.