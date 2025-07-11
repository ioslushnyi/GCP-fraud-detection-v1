'''
This project includes a simple ML model used to predict fraudulent transactions as part of an end-to-end data pipeline. 
This model uses a Random Forest Classifier integrated into a real-time fraud detection pipeline. 
It achieves 95.8% accuracy, with nearly perfect legit user recall (99.9%) and high fraud precision (99.8%) and recall (91.5%). 
The focus here is not on ML complexity, but on showing how a predictive model can be embedded within a scalable, realistic data architecture. 
It balances fraud detection with user trust — a common constraint in real-world systems.'''

import uuid
import random
from faker import Faker
import joblib
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report
import matplotlib.pyplot as plt

# --- Setup ---
fake = Faker()
random.seed(42)
NUM_TRANSACTIONS = 10000
BLACKLISTED_USERS = [str(uuid.uuid4()) for _ in range(200)]
#REPEATED_USERS = str(uuid.uuid4())
REPEATED_USERS = [str(uuid.uuid4()) for _ in range(10)]  # Generate multiple repeated users
# Mapping currencies to countries
CURRENCY_COUNTRY_MAPPING = {
    "USD": ["US"],
    "EUR": ["DE", "FR", "IT", "ES", "NL", "BE", "AT", "SK", "PT", "GR", "FI", "IE", "LU", "CY", "MT"],
    "PLN": ["PL"],
    "GBP": ["UK"],
    "UAH": ["UA"]
}
user_event_buffer = {}

def generateuser_id():
    """Generate a user ID, with a 2% chance of being blacklisted."""
    return (
        str(uuid.uuid4()) if random.random() > 0.1 else
        (random.choice(REPEATED_USERS) if random.random() > 0.02 else random.choice(BLACKLISTED_USERS)) 
    )
# --- Step 1: Data Generator ---
def generate_payment(user_id=None, base_time=None):
    if not user_id:
        user_id = generateuser_id()
    amount = round(random.uniform(5, 20000), 2)
    currency = random.choice(["USD", "EUR", "PLN", "UAH"]) if random.random() > 0.1 else fake.currency_code()
    if currency in CURRENCY_COUNTRY_MAPPING:
        country = random.choice(CURRENCY_COUNTRY_MAPPING[currency]) if random.random() > 0.1 else fake.country_code()
    else:
        country = fake.country_code()
    ip_country = country if random.random() > 0.1 else fake.country_code()
    device = random.choice(["iPhone", "Android", "Windows", "Linux", "Mac"])
    timestamp = base_time or fake.date_time_between(start_date="-30d", end_date="now")
    is_blacklisted = user_id in BLACKLISTED_USERS
    is_large_amount = amount > 10000
    is_suspicious_device = device in ["Windows", "Linux"]
    is_night_time = timestamp.hour < 5 or timestamp.hour > 23
    is_country_currency_mismatch = currency not in CURRENCY_COUNTRY_MAPPING or country not in CURRENCY_COUNTRY_MAPPING[currency]

    is_fraud = int(
        is_blacklisted
        or (is_large_amount and is_country_currency_mismatch and is_suspicious_device and is_night_time)
        or (is_large_amount and currency != "PLN" and is_country_currency_mismatch)
        or (is_large_amount and is_night_time and is_suspicious_device and currency != "PLN")
        or (country != ip_country and is_large_amount and currency != "PLN")
        or (country != ip_country and is_country_currency_mismatch and is_suspicious_device)
    ) if random.random() > 0.1 else 0  # Introduce some randomness in fraud labeling

    return {
        "transaction_id": str(uuid.uuid4()),
        "user_id": user_id,
        "amount": amount,
        "currency": currency,
        "timestamp": timestamp,
        "country": country,
        "ip_country": ip_country,
        "device": device,
        "is_fraud": is_fraud
    }
# --- Step 2: Generate Data ---v
#data = [generate_payment() for _ in range(NUM_TRANSACTIONS)]
data = []
for _ in range(NUM_TRANSACTIONS):
    user_id = generateuser_id()
    base_time = fake.date_time_between(start_date="-30d", end_date="now")
    payment = generate_payment(user_id, base_time)
    data.append(payment)
    #Compute transaction frequency ---
    if user_id not in user_event_buffer:
        user_event_buffer[user_id] = []
    user_event_buffer[user_id].append(payment["timestamp"])
    user_event_buffer[user_id] = [
        t for t in user_event_buffer[user_id]
        if (payment["timestamp"] - t).total_seconds() <= 600
    ]
    payment["txn_count_last_10min"] = len(user_event_buffer[user_id])
    payment["is_fraud"] = int(
        payment["is_fraud"] or payment["txn_count_last_10min"] > 5
    )  # Mark as fraud if more than 5 transactions in the last 10 minutes
    data.append(payment)

df = pd.DataFrame(data)
df["timestamp"] = pd.to_datetime(df["timestamp"])
df["hour"] = df["timestamp"].dt.hour
df = df.sort_values(by=["user_id", "timestamp"])

# --- Step 4: Encode categoricals ---
le_currency = LabelEncoder()
le_country = LabelEncoder()
le_ip_country = LabelEncoder()
le_device = LabelEncoder()

df["currency"] = le_currency.fit_transform(df["currency"])
df["country"] = le_country.fit_transform(df["country"])
df["ip_country"] = le_ip_country.fit_transform(df["ip_country"])
df["device"] = le_device.fit_transform(df["device"])
df = df.fillna(1)

# --- Step 5: Train model ---
df = df.drop(columns=["timestamp", "transaction_id", "user_id"])
X = df.drop("is_fraud", axis=1)
y = df["is_fraud"]
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

model = RandomForestClassifier(n_estimators=100, random_state=42)
model.fit(X_train, y_train)


# Save feature columns in the exact order used during training
feature_order = list(X.columns)
joblib.dump(feature_order, "feature_order_v3.pkl")


feature_importance = model.feature_importances_
features = X.columns

plt.barh(features, feature_importance)
plt.xlabel("Importance")
plt.title("Feature Importance in Fraud Detection Model")
plt.show()

for feat, score in zip(features, feature_importance):
    print(f"{feat}: {score:.4f}")
    
joblib.dump(model, "fraud_model_v3.pkl")
joblib.dump(le_currency, "le_currency_v3.pkl")
joblib.dump(le_country, "le_country_v3.pkl")
joblib.dump(le_ip_country, "le_ip_country_v3.pkl")
joblib.dump(le_device, "le_device_v3.pkl")

y_pred = model.predict(X_test)
print("\n--- Classification Report ---")
print(classification_report(y_test, y_pred, digits=4))
