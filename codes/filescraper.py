import pandas as pd

# Replace with the actual name of your complete scraped file
filename = "suomi24_food_data_2018_2023.csv" 

print("Loading CSV... (this might take a moment depending on the file size)")
df = pd.read_csv(filename)

print("\n=== DATASET METRICS ===")
# Total posts is just the number of rows in the CSV
total_posts = len(df)
print(f"Total posts: {total_posts}")

# Total unique threads
if 'thread_id' in df.columns:
    total_threads = df['thread_id'].nunique()
    print(f"Total unique threads: {total_threads}")
else:
    print("Could not find a 'thread_id' column to count unique threads.")

# Posts per year
if 'year' in df.columns:
    print("\nPosts per year:")
    print(df['year'].value_counts().sort_index())
elif 'date' in df.columns:
    # Extracts the year
    df['parsed_year'] = pd.to_datetime(df['date'], errors='coerce').dt.year
    print("\nPosts per year:")
    # Ignore rows where date was missing or formatted weirdly
    print(df['parsed_year'].dropna().astype(int).value_counts().sort_index())
else:
    print("Could not find a 'year' or 'date' column to sort by year.")