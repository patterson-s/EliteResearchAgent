import json
from pathlib import Path
from load_data import load_dataset

def main():
    dataset_path = Path("data/chunks_dataset.pkl")
    
    print("=" * 80)
    print("Getting all people from dataset")
    print("=" * 80)
    print()
    
    df = load_dataset(dataset_path)
    people = sorted(df['person_name'].unique())
    
    print(f"Found {len(people)} people")
    print()
    print(f"First 10: {', '.join(people[:10])}")
    print(f"Last 10: {', '.join(people[-10:])}")
    
    output_file = Path("all_people.json")
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(people, f, indent=2, ensure_ascii=False)
    
    print(f"\nSaved to: {output_file.resolve()}")
    print("=" * 80)

if __name__ == "__main__":
    main()