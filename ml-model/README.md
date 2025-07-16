This directory contains training code, encoders, and a trained ML model used in the real-time fraud detection pipeline.

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

The fraud detection model is a binary classifier that predicts whether a payment event is fraudulent based on engineered features. It was trained on synthetic data using the following inputs:

- `amount`: Transaction amount
- `currency` (label-encoded): Encoded currency
- `country` (label-encoded): Encoded user country
- `ip_country` (label-encoded): Encoded IP-based country
- `device` (label-encoded): Encoded user device
- `hour` (UTC): Hour of the transaction
- `txn_count_last_10min`: Number of user transactions in the past 10 minutes

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
