from kafka import KafkaProducer
from kafka.sasl.oauth import AbstractTokenProvider
import json
import time
import random
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

# MSK Configuration
BOOTSTRAP_SERVERS = [
    'b-3.workingmultitableclus.nhe1pt.c2.kafka.ap-south-1.amazonaws.com:9098',
    'b-1.workingmultitableclus.nhe1pt.c2.kafka.ap-south-1.amazonaws.com:9098',
    'b-2.workingmultitableclus.nhe1pt.c2.kafka.ap-south-1.amazonaws.com:9098'
]
AWS_REGION = 'ap-south-1'

# Token provider
class MSKTokenProvider(AbstractTokenProvider):
    def token(self):
        token, _ = MSKAuthTokenProvider.generate_auth_token(AWS_REGION)
        return token

# Create producer
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    security_protocol='SASL_SSL',
    sasl_mechanism='OAUTHBEARER',
    sasl_oauth_token_provider=MSKTokenProvider(),
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    request_timeout_ms=60000,
    api_version_auto_timeout_ms=10000,
    max_block_ms=60000
)

# Topic configurations
TOPICS_CONFIG = {
    'user_events': {
        'event_types': ['surge_viewed', 'ride_requested', 'driver_assigned', 
                       'driver_profile_viewed', 'fare_viewed', 'ride_cancelled'],
        'color': '\033[94m'  # Blue
    },
    'payment_events': {
        'event_types': ['payment_initiated', 'payment_completed', 'payment_failed',
                       'refund_requested', 'refund_processed', 'wallet_updated'],
        'color': '\033[92m'  # Green
    },
    'driver_events': {
        'event_types': ['driver_online', 'driver_offline', 'ride_accepted',
                       'ride_started', 'ride_completed', 'location_updated'],
        'color': '\033[93m'  # Yellow
    }
}

# Shared data
user_ids = [f'user_{i}' for i in range(1, 101)]
driver_ids = [f'driver_{i}' for i in range(1, 51)]

# Reset color
RESET = '\033[0m'

def generate_user_event():
    """Generate user event"""
    return {
        'event_id': f'evt_{int(time.time()*1000)}_{random.randint(1000,9999)}',
        'user_id': random.choice(user_ids),
        'event_type': random.choice(TOPICS_CONFIG['user_events']['event_types']),
        'timestamp': int(time.time() * 1000),
        'ride_id': f'ride_{random.randint(10000, 99999)}',
        'metadata': {
            'surge_multiplier': round(random.uniform(1.0, 3.0), 1),
            'estimated_wait_minutes': random.randint(2, 15),
            'fare_amount': random.randint(100, 800),
            'driver_rating': round(random.uniform(3.5, 5.0), 1)
        }
    }

def generate_payment_event():
    """Generate payment event"""
    return {
        'event_id': f'pay_{int(time.time()*1000)}_{random.randint(1000,9999)}',
        'user_id': random.choice(user_ids),
        'event_type': random.choice(TOPICS_CONFIG['payment_events']['event_types']),
        'timestamp': int(time.time() * 1000),
        'ride_id': f'ride_{random.randint(10000, 99999)}',
        'metadata': {
            'surge_multiplier': round(random.uniform(1.0, 3.0), 1),
            'estimated_wait_minutes': random.randint(2, 15),
            'fare_amount': random.randint(100, 800),
            'driver_rating': round(random.uniform(3.5, 5.0), 1)
        }
    }

def generate_driver_event():
    """Generate driver event"""
    return {
        'event_id': f'drv_{int(time.time()*1000)}_{random.randint(1000,9999)}',
        'user_id': random.choice(driver_ids),
        'event_type': random.choice(TOPICS_CONFIG['driver_events']['event_types']),
        'timestamp': int(time.time() * 1000),
        'ride_id': f'ride_{random.randint(10000, 99999)}',
        'metadata': {
            'surge_multiplier': round(random.uniform(1.0, 3.0), 1),
            'estimated_wait_minutes': random.randint(2, 15),
            'fare_amount': random.randint(100, 800),
            'driver_rating': round(random.uniform(3.5, 5.0), 1)
        }
    }

# Event generators mapping
EVENT_GENERATORS = {
    'user_events': generate_user_event,
    'payment_events': generate_payment_event,
    'driver_events': generate_driver_event
}

print("="*60)
print("Starting Multi-Topic Kafka Producer")
print("="*60)
print(f"{TOPICS_CONFIG['user_events']['color']}USER EVENTS{RESET} | "
      f"{TOPICS_CONFIG['payment_events']['color']}PAYMENT EVENTS{RESET} | "
      f"{TOPICS_CONFIG['driver_events']['color']}DRIVER EVENTS{RESET}")
print("="*60)

try:
    counter = 0
    while True:
        # Randomly select a topic
        topic = random.choice(list(TOPICS_CONFIG.keys()))
        
        # Generate event for that topic
        event = EVENT_GENERATORS[topic]()
        
        # Send to Kafka
        producer.send(topic, value=event)
        
        # Display
        color = TOPICS_CONFIG[topic]['color']
        print(f"{color}[{topic:15s}]{RESET} "
              f"{event['event_type']:25s} | {event['user_id']:10s} | "
              f"ID: {event['event_id']}")
        
        counter += 1
        if counter % 10 == 0:
            print(f"\n--- {counter} events sent ---\n")
        
        # Random delay between events
        time.sleep(random.uniform(0.3, 1.5))
        
except KeyboardInterrupt:
    print("\n" + "="*60)
    print(f"Stopped after {counter} events")
    print("="*60)
finally:
    producer.close()