from queries.fetch_jobsteps import fetch_all_jobsteps

INPUT_CSV = "input/job_guids.csv"
OUTPUT_CSV = "output/jobsteps_result.csv"


def main():
    print("Agent started.")
    df = fetch_all_jobsteps(INPUT_CSV)

    if df.empty:
        print("No data returned.")
        return

    print(f"Fetched {len(df)} rows.")
    print(df.head())

    df.to_csv(OUTPUT_CSV, index=False)
    print(f"Results saved to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
