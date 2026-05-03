import pandas as pd

print("Loading the filtered dataset...")
# Load the master file generated with sisyphus.py
df = pd.read_csv("suomi24_food_data_2018_2023.csv")

# Convert the timestamp string into a readable datetime format
df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')

# Extract just the year into a new temporary column
df['year'] = df['timestamp'].dt.year

# Drop rows where the timestamp was missing or broken, then format as integer
df = df.dropna(subset=['year'])
df['year'] = df['year'].astype(int)

print(f"Found years: {sorted(df['year'].unique())}")
print("Partitioning files...")

# Loop through the 6 years 2018-2023
for year in sorted(df['year'].unique()):
    # Create a sub-dataframe containing only data from that specific year
    df_yearly = df[df['year'] == year]
    
    # Define new file name
    output_filename = f"suomi24_food_{year}.csv"
    
    # Drop temporary year column
    df_yearly = df_yearly.drop(columns=['year'])
    
    # Save the chunk to a new CSV file
    df_yearly.to_csv(output_filename, index=False, encoding='utf-8-sig')
    
    print(f"Successfully saved {len(df_yearly)} posts to {output_filename}")

print("Partitioning complete!")
