import pandas as pd
from collections import Counter
from glob import glob

file_paths = glob('*.csv')  # Update path

chunks = []

for file_path in file_paths:
    for chunk in pd.read_csv(file_path, chunksize=5000):
        chunks.append(chunk)

df = pd.concat(chunks, ignore_index=True)

# dataframes = [pd.read_csv(file) for file in file_paths]
# df = pd.concat(dataframes, ignore_index=True) # Combining CSVs

# Load the dataset
# file_path = "2004_adverse_drug_reactions_extended.csv"
# df = pd.read_csv(file_path)

# Drop rows where Age or Sex is missing
df = df.dropna(subset=['Age', 'Sex'])

# Convert Age to numeric, invalid values to NaN
df['Age'] = pd.to_numeric(df['Age'], errors='coerce')

# Exclude rows where Age is below 18 or above 85
df = df[(df['Age'] >= 18) & (df['Age'] <= 85)]

# Map Age to Age Group
def categorize_age(age):
    if 18 <= age <= 35:
        return 'young adult'
    elif 36 <= age <= 55:
        return 'middle-aged adult'
    elif 56 <= age <= 85:
        return 'older adult'

df['Age Group'] = df['Age'].apply(lambda x: categorize_age(x))

# Split the 'Reactions' column into a list of reactions and convert to lowercase
df['Reactions'] = df['Reactions'].str.lower().str.split(', ')

# Initialize an empty list for processed data
processed_data = []

# Group by Drug Name, Age Group, and Sex
for (drug, age_group, sex), group in df.groupby(['Drug Name', 'Age Group', 'Sex']):
    # Flatten the list of reactions for the group
    all_reactions = [reaction for sublist in group['Reactions'] for reaction in sublist]
    
    # Count frequencies of each reaction
    reaction_counts = Counter(all_reactions)
    
    # Add a row with frequencies for this group
    row = {'Drug Name': drug, 'Age Group': age_group, 'Sex': sex}
    row.update(reaction_counts)  # Add reaction frequencies as columns
    processed_data.append(row)

# Convert the processed data into a DataFrame
processed_df = pd.DataFrame(processed_data)

# Fill NaN values with 0 (for reactions that are missing in some rows)
processed_df = processed_df.fillna(0)

# Format column names to replace periods with spaces
processed_df.columns = processed_df.columns.str.replace('.', ' ', regex=False)

# Save the result to a new CSV file
processed_df.to_csv("processed_adverse_drug_reactions_by_age_group1.csv", index=False)

print("Processing complete. Output saved to 'processed_adverse_drug_reactions_by_age_group1.csv'.")

