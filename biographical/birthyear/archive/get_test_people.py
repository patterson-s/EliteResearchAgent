import json
from pathlib import Path
from load_data import load_dataset

def main():
    dataset_path = Path("data/chunks_dataset.pkl")
    
    print("=" * 80)
    print("Getting first 5 test people from dataset")
    print("=" * 80)
    print()
    
    df = load_dataset(dataset_path)
    people = sorted(df['person_name'].unique())[:5]
    
    print(f"Found {len(people)} people:")
    for i, name in enumerate(people, 1):
        print(f"  {i}. {name}")
    
    output_file = Path("test_people.json")
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(people, f, indent=2, ensure_ascii=False)
    
    print(f"\nSaved to: {output_file.resolve()}")
    print("=" * 80)

if __name__ == "__main__":
    main()