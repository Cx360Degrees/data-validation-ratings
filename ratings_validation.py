import pandas as pd

def process_file(file_path):
    """Basic function to load a file and display its content."""
    df = pd.read_csv(file_path)
    print("Data Preview:")
    print(df.head())

if __name__ == "__main__":
    sample_file = "sample.csv"  # Change this to test
    process_file(sample_file)
