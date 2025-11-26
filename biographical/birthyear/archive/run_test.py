import subprocess
from pathlib import Path

def main():
    print("=" * 100)
    print("Birth Year Service - Test Workflow")
    print("=" * 100)
    print()
    
    print("Step 1: Load dataset from database")
    print("-" * 100)
    subprocess.run([
        "python", "load_data.py",
        "--output", "data/chunks_dataset.pkl",
        "--stats"
    ], check=True)
    print()
    
    print("Step 2: Get first 5 test people")
    print("-" * 100)
    subprocess.run(["python", "get_test_people.py"], check=True)
    print()
    
    print("Step 3: Run batch verification")
    print("-" * 100)
    subprocess.run([
        "python",
        "batch.py",
        "test_people.json",
        "--output", "review"
    ], check=True)
    print()
    
    print("Step 4: Generate summary report")
    print("-" * 100)
    subprocess.run(["python", "summarize_results.py"], check=True)
    print()
    
    print("=" * 100)
    print("Test complete!")
    print("=" * 100)
    print()
    print("Review individual results in: review/")
    print("Batch summary in: review/batch_summary_*.json")

if __name__ == "__main__":
    main()