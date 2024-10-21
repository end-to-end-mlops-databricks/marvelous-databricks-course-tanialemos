import pandas as pd

def print_hello():
    print("hello from preprocessing!")

def load_data(filepath):
    try:
        df = pd.read_csv(filepath)
    except Exception as e:
        print(f"Yikes! An error ocurred: {e}")

    print(f"df shape: {df.shape}")