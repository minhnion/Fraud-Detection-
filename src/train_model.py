import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
import joblib 
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def train_and_save_model():
    try:
        df = pd.read_csv('data/creditcard.csv')
        logging.info("Successfully loaded creditcard.csv")
    except FileNotFoundError:
        logging.error("data/creditcard.csv not found. Please make sure the file exists.")
        return


    df = df.drop('Time', axis=1)

    X = df.drop('Class', axis=1)
    y = df['Class']
    

    X_sample, _, y_sample, _ = train_test_split(X, y, test_size=0.95, random_state=42, stratify=y)
    logging.info(f"Training on a sample of {len(X_sample)} records.")


    pipeline = Pipeline([
        ('scaler', StandardScaler()),
        ('classifier', RandomForestClassifier(n_estimators=100, random_state=42, n_jobs=-1))
    ])

    logging.info("Starting model training...")
    pipeline.fit(X_sample, y_sample)
    logging.info("Model training completed.")

    import os
    os.makedirs('data/model', exist_ok=True)
    model_path = 'data/model/sklearn_fraud_model.pkl'
    joblib.dump(pipeline, model_path)
    logging.info(f"Model saved successfully to {model_path}")

if __name__ == "__main__":
    train_and_save_model()