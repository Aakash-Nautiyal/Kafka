from flask import Flask, request, jsonify
from confluent_kafka import Producer
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
import json

# ----------------------------
# Flask App
# ----------------------------
app = Flask(__name__)

# ----------------------------
# IAM OAuth Callback (IMPORTANT)
# ----------------------------
def oauth_cb(oauth_config):
    token, expiry = MSKAuthTokenProvider.generate_auth_token("ap-northeast-1")
    return token, expiry

# ----------------------------
# Kafka Configuration
# ----------------------------
conf = {
    'bootstrap.servers': 'boot-ctv8ahsg.c1.kafka-serverless.ap-northeast-1.amazonaws.com:9098',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'OAUTHBEARER',
    'oauth_cb': oauth_cb,

    # Optional but useful
    'client.id': 'flask-producer'
}

# ----------------------------
# Create Kafka Producer
# ----------------------------
producer = Producer(conf)

# ----------------------------
# Delivery Callback (optional)
# ----------------------------
def delivery_report(err, msg):
    if err:
        print(f"❌ Message failed: {err}")
    else:
        print(f"✅ Message delivered to {msg.topic()} [{msg.partition()}]")

# ----------------------------
# API Endpoint
# ----------------------------
@app.route('/order', methods=['POST'])
def create_order():
    data = request.get_json()

    email = data.get('email')
    order = data.get('order')

    print(f"Email: {email}")
    print(f"Order: {order}")

    try:
        producer.produce(
            topic='order',
            value=json.dumps(data),
            callback=delivery_report
        )
        producer.flush()

        return jsonify({
            "status": "success",
            "message": "sent to kafka",
            "data": data
        })

    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500


# ----------------------------
# Run Server
# ----------------------------
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)
