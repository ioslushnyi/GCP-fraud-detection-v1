This directory contains training code, encoders, and a trained ML model used in the real-time fraud detection pipeline.
The fraud detection model is a binary classifier (Random Forest Classifier) that predicts whether a payment event is fraudulent based on engineered features. 

## Contents

- `train_model.py`: Script used to train the fraud detection model
- `test_predict.py`: Script for testing predictions on a sample input
- `fraud_model.pkl`: Trained RandomForestClassifier model (saved with joblib)
- `feature_order.pkl`: Ordered list of features expected by the model
- Label encoders:
  - `le_country.pkl`
  - `le_currency.pkl`
  - `le_ip_country.pkl`
  - `le_device.pkl`
- `classification_report.txt`: Model evaluation summary
- `feature_importance.png`: Visualized feature importance chart

## Fraud Detection Logic
The model is trained on synthetic payment data generated with [Faker](https://faker.readthedocs.io/en/master/) and custom fraud rules. These rules emulate common fraud patterns.

### Labeling Logic
An event is labeled as fraud (`is_fraud = 1`) if any of the following conditions are met (with added randomness for realism):
- The `user_id` is in a manually generated blacklist
- The payment amount is large (> 10,000), and one or more of the following apply:
  - Device is suspicious (e.g., "Windows" or "Linux")
  - Transaction occurs at night (hour < 5 or > 23)
  - Country/currency mismatch (e.g., "USD" in "PL")
  - IP country differs from billing country
- User has made > 5 transactions within 10 minutes\
A 10% randomness is introduced to reduce model overfitting to limited patterns.

### Input Features
It was trained on the following features:
- `amount`: Transaction amount
- `currency` (label-encoded): Encoded currency
- `country` (label-encoded): Encoded user country
- `ip_country` (label-encoded): Encoded IP-based country
- `device` (label-encoded): Encoded user device
- `hour` (UTC): Hour of the transaction
- `txn_count_last_10min`: Number of user transactions in the past 10 minutes

### Output
The model outputs a fraud probability (`fraud_score`) and a binary label (`fraud_label` = 1 if `fraud_score` > 0.5).

Risk level is also inferred from `fraud_score`:
| Fraud Score Range  | Risk Level |
| ------------- | ------------- |
| >= 0.90	  | critical  |
| >= 0.70	  | high  |
| >= 0.40	  | medium  |
| >= 0.10	  | low  |
| < 0.10	  | minimal  |

## How to Re-Train
```
python train_model.py
```
## How to test
You can use `test_predict.py` and modify event to test model
```
python test_predict.py
```
