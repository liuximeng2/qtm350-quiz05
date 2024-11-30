import os
import pandas as pd
import numpy as np
import glob

# Create a directory to store Parquet files
os.makedirs('data', exist_ok=True)

# Years from 1945 to 2023
years = np.arange(1945, 2024)

# Define approximate GDP per capita and population ranges for each country
# Based on historical data (approximate values)
country_data = {
    'Brazil': {
        'gdp_start': 1000,    # GDP per capita in 1945 (USD)
        'gdp_end': 10000,     # GDP per capita in 2023 (USD)
        'pop_start': 45_000_000,    # Population in 1945
        'pop_end': 210_000_000      # Population in 2023
    },
    'India': {
        'gdp_start': 500,
        'gdp_end': 2000,
        'pop_start': 340_000_000,
        'pop_end': 1_390_000_000
    },
    'USA': {
        'gdp_start': 15_000,
        'gdp_end': 65_000,
        'pop_start': 140_000_000,
        'pop_end': 330_000_000
    },
    'UK': {
        'gdp_start': 10_000,
        'gdp_end': 45_000,
        'pop_start': 50_000_000,
        'pop_end': 67_000_000
    }
}

# Generate data for each country
for country, info in country_data.items():
    np.random.seed(hash(country) % 1234567)  # Seed for reproducibility
    num_years = len(years)

    # Linear interpolation of GDP per capita and population over the years
    gdp_per_capita = np.linspace(info['gdp_start'], info['gdp_end'], num_years)
    population = np.linspace(info['pop_start'], info['pop_end'], num_years)

    # Add some random variation to GDP per capita
    gdp_noise = np.random.normal(0, gdp_per_capita * 0.05)  # 5% standard deviation
    gdp_per_capita += gdp_noise

    # Add some random variation to population
    pop_noise = np.random.normal(0, population * 0.02)  # 2% standard deviation
    population += pop_noise

    # Ensure population is positive and convert to integers
    population = np.abs(population.astype(int))

    # Create DataFrame
    data = pd.DataFrame({
        'country': country,
        'year': years,
        'gdp_per_capita': gdp_per_capita,
        'population': population
    })

    # Save to Parquet file
    data.to_parquet(f'data/{country}.parquet', index=False)

    print(f"Data for {country} saved to data/{country}.parquet")

def combine_parquet_files(parquet_dir, output_csv):
    # Combine multiple Parquet files into a single CSV file

    # Use glob to find all .parquet files in the directory
    parquet_files = glob.glob(os.path.join(parquet_dir, '*.parquet'))

    # Sort the files alphabetically
    parquet_files.sort()

    if not parquet_files:
        print(f"No Parquet files found in directory '{parquet_dir}'.")
        return

    all_data = []
    for file in parquet_files:
        print(f"Processing {file}")
        df = pd.read_parquet(file)
        all_data.append(df)

    # Concatenate all DataFrames
    combined_df = pd.concat(all_data, ignore_index=True)

    # Construct the output path within the parquet_dir
    output_csv_path = os.path.join(parquet_dir, output_csv)

    # Save the combined DataFrame as a CSV file inside the parquet_dir
    combined_df.to_csv(output_csv_path, index=False)
    print(f"All Parquet files combined and saved as '{output_csv_path}'.")

# Call the function after generating individual Parquet files
combine_parquet_files('data', 'data.csv')
