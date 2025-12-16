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

# Token provider - inherit from AbstractTokenProvider
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

# Event data
event_types = ['surge_viewed', 'ride_requested', 'driver_assigned', 
               'driver_profile_viewed', 'fare_viewed', 'ride_cancelled']
user_ids = [f'user_{i}' for i in range(1, 101)]

print("Starting producer...")
try:
    while True:
        event = {
            'event_id': f'evt_{int(time.time()*1000)}_{random.randint(1000,9999)}',
            'user_id': random.choice(user_ids),
            'event_type': random.choice(event_types),
            'timestamp': int(time.time() * 1000),
            'ride_id': f'ride_{random.randint(10000, 99999)}',
            'metadata': {
                'surge_multiplier': round(random.uniform(1.0, 3.0), 1),
                'estimated_wait_minutes': random.randint(2, 15),
                'fare_amount': random.randint(100, 800),
                'driver_rating': round(random.uniform(3.5, 5.0), 1)
            }
        }
        
        producer.send('user_events', value=event)
        print(f"✓ {event['event_type']} | {event['user_id']}")
        time.sleep(random.uniform(0.5, 2))
        
except KeyboardInterrupt:
    print("\nStopped")
finally:
    producer.close()