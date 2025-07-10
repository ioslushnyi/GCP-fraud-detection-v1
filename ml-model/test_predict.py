import joblib
import pandas as pd
from datetime import datetime, timezone

# Load model and pre-fitted encoders (these should be saved during training)
model = joblib.load("fraud_model_v3.pkl")
le_currency = joblib.load("le_currency_v3.pkl")
le_country = joblib.load("le_country_v3.pkl")
le_ip_country = joblib.load("le_ip_country_v3.pkl")
le_device = joblib.load("le_device_v3.pkl")
# Load expected feature order
feature_order = joblib.load("feature_order_v3.pkl")

# Simulated incoming event
custom_event = {
    "user_id": "user_2",
    "timestamp": datetime.now(timezone.utc),
    "amount": 9999.0,
    "currency": "EUR",
    "country": "PL",
    "ip_country": "DE",
    "device": "Linux"
}

# Encode categoricals (fallback to -1 for unknown)
def safe_encode(encoder, value):
    return encoder.transform([value])[0] if value in encoder.classes_ else -1

# Format as model input
features = pd.DataFrame([{
    "amount": custom_event["amount"],
    "currency": safe_encode(le_currency, custom_event["currency"]),
    "country": safe_encode(le_country, custom_event["country"]),
    "ip_country": safe_encode(le_ip_country, custom_event["ip_country"]),
    "device": safe_encode(le_device, custom_event["device"]),
    "hour": custom_event["timestamp"].hour,
    "txn_count_last_10min": 5
}])
print("\n--- Input Features ---")
print(features)
# Inference
risk_score = model.predict_proba(features[feature_order])[0][1]
predicted_label = int(risk_score > 0.5)

# Risk bucket
risk_level = (
    "critical" if risk_score > 0.9 else
    "high" if risk_score > 0.7 else
    "medium" if risk_score > 0.4 else
    "low"
)

# Output
print("\n--- Custom Event Prediction ---")
print(features)
print(f"Risk Score: {risk_score:.4f}")
print(f"Predicted Fraud: {predicted_label} ({risk_level})")
