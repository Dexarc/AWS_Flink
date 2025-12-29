from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.sasl.oauth import AbstractTokenProvider
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
import json
import time
import random

# MSK Configuration
BOOTSTRAP_SERVERS = [
    'b-3.workingmultitableclus.nhe1pt.c2.kafka.ap-south-1.amazonaws.com:9098',
    'b-1.workingmultitableclus.nhe1pt.c2.kafka.ap-south-1.amazonaws.com:9098',
    'b-2.workingmultitableclus.nhe1pt.c2.kafka.ap-south-1.amazonaws.com:9098'
]
AWS_REGION = 'ap-south-1'
TOPIC_NAME = 'bid-events'

class MSKTokenProvider(AbstractTokenProvider):
    def token(self):
        token, _ = MSKAuthTokenProvider.generate_auth_token(AWS_REGION)
        return token

# Create topic if not exists
try:
    admin = KafkaAdminClient(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        security_protocol='SASL_SSL',
        sasl_mechanism='OAUTHBEARER',
        sasl_oauth_token_provider=MSKTokenProvider()
    )
    
    topic = NewTopic(name=TOPIC_NAME, num_partitions=3, replication_factor=2)
    admin.create_topics([topic])
    print(f"✓ Topic '{TOPIC_NAME}' created")
    admin.close()
except Exception as e:
    print(f"Topic exists or error: {e}")

# Create producer
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    security_protocol='SASL_SSL',
    sasl_mechanism='OAUTHBEARER',
    sasl_oauth_token_provider=MSKTokenProvider(),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print(f"Streaming events to '{TOPIC_NAME}'...")

try:
    while True:
        # FLAT structure - no "success" wrapper
        event = {
            "event_name": random.choice(["bid_booking_timeout", "ride_requested", "driver_assigned"]),
            "user_id": random.randint(10000, 99999),
            "city_id": random.randint(1, 10),
            "platform": random.choice(["ios", "android", "web"]),
            "session_id": f"{int(time.time())}{random.randint(10000, 99999)}",
            "flow_id": f"{int(time.time())}{random.randint(10000, 99999)}",
            "timestamp_ms": int(time.time() * 1000)
        }
        
        producer.send(TOPIC_NAME, value=event)
        print(f"✓ {event['event_name']} | user: {event['user_id']}")
        
        time.sleep(random.uniform(0.5, 2))
        
except KeyboardInterrupt:
    print("\nStopped")
finally:
    producer.close()