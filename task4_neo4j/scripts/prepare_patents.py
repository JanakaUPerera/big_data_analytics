import csv

INPUT_FILE = "/data/cit-Patents.txt"
OUTPUT_FILE = "/import/patent_citations_5000.csv"

LIMIT = 5000


def main():
    count = 0

    with open(INPUT_FILE, "r", encoding="utf-8") as source, \
         open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as target:

        writer = csv.writer(target)

        writer.writerow([
            "source_patent",
            "cited_patent"
        ])

        for line in source:
            line = line.strip()

            if not line or line.startswith("#"):
                continue

            parts = line.split()

            if len(parts) < 2:
                continue

            source_patent = parts[0]
            cited_patent = parts[1]

            writer.writerow([
                source_patent,
                cited_patent
            ])

            count += 1

            if count >= LIMIT:
                break

    print(f"Created {OUTPUT_FILE}")
    print(f"Citation relationships written: {count}")


if __name__ == "__main__":
    main()