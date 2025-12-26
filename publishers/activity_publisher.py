"""
Activity Publisher for Quest System
Publishes user activity events from Station to Quest system
"""

import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional
import pika

logger = logging.getLogger(__name__)


class ActivityPublisher:
    """
    Publishes user activity events to RabbitMQ
    For Quest system to track user behavior
    """
    
    EXCHANGE_NAME = 'user_activities'
    EXCHANGE_TYPE = 'topic'
    
    # Event routing keys mapping
    ROUTING_KEYS = {
        # Vault events
        'CREATE_VAULT': 'user.vault.create',
        'DEPOSIT_FUNDS': 'user.vault.deposit',
        'WITHDRAW_FUNDS': 'user.vault.withdraw',
        
        # Flow events
        'CREATE_FLOW': 'user.flow.create',
        'RUN_FLOW': 'user.flow.run',
        'COMPLETE_FLOW': 'user.flow.complete',
        'RUN_NODE': 'user.node.run',
        
        # Community events
        'CREATE_COMMUNITY_NODE': 'user.community.create_node',
        'STAR_COMMUNITY_NODE': 'user.community.star',
        'COMMENT_COMMUNITY_NODE': 'user.community.comment',
        
        # Trading events
        'SWAP_TOKEN': 'user.trading.swap',
        'BUY_TOKEN': 'user.trading.buy',
        'SELL_TOKEN': 'user.trading.sell',
        'EARN_PROFIT': 'user.trading.profit',
        
        # Other events
        'CONNECT_WALLET': 'user.profile.connect',
        'COMPLETE_PROFILE': 'user.profile.complete',
        'INVITE_USER': 'user.social.invite',
        'VISIT_PAGE': 'user.visit.page',
    }
    
    def __init__(self, connection: pika.BlockingConnection):
        """
        Initialize Activity Publisher
        
        Args:
            connection: RabbitMQ connection instance
        """
        self.connection = connection
        self.channel = None
        self._setup_exchange()
    
    def _setup_exchange(self):
        """Setup Exchange"""
        try:
            self.channel = self.connection.channel()
            self.channel.exchange_declare(
                exchange=self.EXCHANGE_NAME,
                exchange_type=self.EXCHANGE_TYPE,
                durable=True
            )
            logger.info(f'Activity Publisher: Exchange "{self.EXCHANGE_NAME}" declared')
        except Exception as e:
            logger.error(f'Failed to setup exchange: {e}')
            raise
    
    def publish(
        self,
        user_id: str,
        event_type: str,
        metadata: Dict[str, Any] = None,
        source: str = 'station'
    ) -> bool:
        """
        Publish user activity event
        
        Args:
            user_id: User ID
            event_type: Event type (e.g. 'CREATE_VAULT')
            metadata: Event metadata
            source: Event source ('station', 'control', 'frontend')
        
        Returns:
            bool: Whether publish was successful
        """
        try:
            # Get routing key
            routing_key = self.ROUTING_KEYS.get(event_type, 'user.unknown')
            
            # Build message
            message = {
                'userId': user_id,
                'eventType': event_type,
                'metadata': metadata or {},
                'source': source,
                'timestamp': datetime.utcnow().isoformat(),
            }
            
            # Publish message
            self.channel.basic_publish(
                exchange=self.EXCHANGE_NAME,
                routing_key=routing_key,
                body=json.dumps(message),
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Make message persistent
                    content_type='application/json',
                )
            )
            
            logger.info(
                f'Activity published: {event_type} for user {user_id} '
                f'(routing_key: {routing_key})'
            )
            return True
            
        except Exception as e:
            logger.error(f'Failed to publish activity: {e}')
            return False
    
    def publish_flow_run(
        self,
        user_id: str,
        flow_id: str,
        cycle: int,
        metadata: Dict[str, Any] = None
    ):
        """
        Publish Flow run event
        
        Args:
            user_id: User ID
            flow_id: Flow ID
            cycle: Run cycle
            metadata: Additional metadata
        """
        event_metadata = {
            'flowId': flow_id,
            'cycle': cycle,
            **(metadata or {})
        }
        return self.publish(user_id, 'RUN_FLOW', event_metadata)
    
    def publish_flow_complete(
        self,
        user_id: str,
        flow_id: str,
        cycle: int,
        success: bool,
        metadata: Dict[str, Any] = None
    ):
        """
        Publish Flow complete event
        
        Args:
            user_id: User ID
            flow_id: Flow ID
            cycle: Run cycle
            success: Whether successful
            metadata: Additional metadata
        """
        event_metadata = {
            'flowId': flow_id,
            'cycle': cycle,
            'success': success,
            **(metadata or {})
        }
        return self.publish(user_id, 'COMPLETE_FLOW', event_metadata)
    
    def publish_node_run(
        self,
        user_id: str,
        node_type: str,
        flow_id: str,
        metadata: Dict[str, Any] = None
    ):
        """
        Publish node run event
        
        Args:
            user_id: User ID
            node_type: Node type
            flow_id: Flow ID
            metadata: Additional metadata
        """
        event_metadata = {
            'nodeType': node_type,
            'flowId': flow_id,
            **(metadata or {})
        }
        return self.publish(user_id, 'RUN_NODE', event_metadata)
    
    def close(self):
        """Close connection"""
        try:
            if self.channel and not self.channel.is_closed:
                self.channel.close()
            logger.info('Activity Publisher closed')
        except Exception as e:
            logger.error(f'Error closing Activity Publisher: {e}')


# Global singleton
_publisher_instance: Optional[ActivityPublisher] = None


def init_activity_publisher(connection: pika.BlockingConnection) -> ActivityPublisher:
    """
    Initialize global Activity Publisher instance
    
    Args:
        connection: RabbitMQ connection
    
    Returns:
        ActivityPublisher instance
    """
    global _publisher_instance
    if _publisher_instance is None:
        _publisher_instance = ActivityPublisher(connection)
        logger.info('Global Activity Publisher initialized')
    return _publisher_instance


def get_activity_publisher() -> Optional[ActivityPublisher]:
    """
    Get global Activity Publisher instance
    
    Returns:
        ActivityPublisher instance, or None if not initialized
    """
    return _publisher_instance


def publish_activity(
    user_id: str,
    event_type: str,
    metadata: Dict[str, Any] = None
) -> bool:
    """
    Convenience method: Publish activity event
    
    Args:
        user_id: User ID
        event_type: Event type
        metadata: Metadata
    
    Returns:
        Whether publish was successful
    """
    publisher = get_activity_publisher()
    if publisher:
        return publisher.publish(user_id, event_type, metadata)
    else:
        logger.warning('Activity Publisher not initialized, skipping event publication')
        return False
