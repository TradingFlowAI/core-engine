"""
Trading Signal Publisher
Station 发布交易信号到 MQ
"""

import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional
import pika

logger = logging.getLogger(__name__)


class TradingSignalPublisher:
    """发布交易信号到 RabbitMQ"""
    
    def __init__(self, rabbitmq_url: str = None):
        """
        Initialize publisher
        
        Args:
            rabbitmq_url: RabbitMQ connection URL
        """
        self.rabbitmq_url = rabbitmq_url or 'amqp://guest:guest@localhost:5672/'
        self.exchange_name = 'trading_signals'
        self.connection = None
        self.channel = None
        
    def connect(self):
        """Connect to RabbitMQ"""
        try:
            parameters = pika.URLParameters(self.rabbitmq_url)
            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()
            
            # Declare exchange
            self.channel.exchange_declare(
                exchange=self.exchange_name,
                exchange_type='topic',
                durable=True
            )
            
            logger.info(f"[TradingSignalPublisher] Connected to RabbitMQ: {self.exchange_name}")
            
        except Exception as e:
            logger.error(f"[TradingSignalPublisher] Connection error: {e}")
            raise
    
    def publish_signal(
        self,
        flow_id: str,
        user_id: str,
        vault_address: str,
        signal_type: str,
        signal_data: Dict[str, Any],
        node_id: Optional[str] = None,
        node_type: Optional[str] = None
    ) -> bool:
        """
        Publish trading signal
        
        Args:
            flow_id: Flow ID
            user_id: User ID
            vault_address: Vault address
            signal_type: Signal type (swap, buy, sell, etc.)
            signal_data: Signal data dict
            node_id: Node ID
            node_type: Node type
            
        Returns:
            bool: Success status
        """
        try:
            if not self.channel:
                self.connect()
            
            message = {
                'flowId': flow_id,
                'userId': user_id,
                'vaultAddress': vault_address,
                'signalType': signal_type,
                'signalData': signal_data,
                'nodeId': node_id,
                'nodeType': node_type,
                'timestamp': datetime.utcnow().isoformat()
            }
            
            # Determine routing key
            routing_key = f"signal.trade.{signal_type}"
            
            # Publish message
            self.channel.basic_publish(
                exchange=self.exchange_name,
                routing_key=routing_key,
                body=json.dumps(message),
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Persistent
                    content_type='application/json'
                )
            )
            
            logger.info(
                f"[TradingSignalPublisher] Published {signal_type} signal for flow {flow_id}, "
                f"routing_key={routing_key}"
            )
            
            return True
            
        except Exception as e:
            logger.error(f"[TradingSignalPublisher] Publish error: {e}")
            return False
    
    def close(self):
        """Close connection"""
        try:
            if self.connection and not self.connection.is_closed:
                self.connection.close()
                logger.info("[TradingSignalPublisher] Connection closed")
        except Exception as e:
            logger.error(f"[TradingSignalPublisher] Close error: {e}")


# Global publisher instance
_publisher_instance: Optional[TradingSignalPublisher] = None


def get_signal_publisher() -> TradingSignalPublisher:
    """Get global publisher instance (singleton)"""
    global _publisher_instance
    
    if _publisher_instance is None:
        _publisher_instance = TradingSignalPublisher()
    
    return _publisher_instance


def publish_trading_signal(
    flow_id: str,
    user_id: str,
    vault_address: str,
    signal_type: str,
    signal_data: Dict[str, Any],
    node_id: Optional[str] = None,
    node_type: Optional[str] = None
) -> bool:
    """
    Convenience function to publish trading signal
    
    Args:
        flow_id: Flow ID
        user_id: User ID  
        vault_address: Vault address
        signal_type: Signal type
        signal_data: Signal data
        node_id: Node ID
        node_type: Node type
        
    Returns:
        bool: Success status
    """
    publisher = get_signal_publisher()
    
    return publisher.publish_signal(
        flow_id=flow_id,
        user_id=user_id,
        vault_address=vault_address,
        signal_type=signal_type,
        signal_data=signal_data,
        node_id=node_id,
        node_type=node_type
    )
