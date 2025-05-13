import pandas as pd
from glob import glob
from collections import Counter
from concurrent.futures import ProcessPoolExecutor

# Load unique reactions
unique_reactions = pd.read_csv('unique_reactions.csv')['Reaction'].tolist()

# Output file name
output_file = 'processed_adverse_drug_reactions.csv'

# Write header to the output file
columns = ['Drug Name', 'Age Group', 'Sex'] + unique_reactions
pd.DataFrame(columns=columns).to_csv(output_file, index=False)

def process_chunk(chunk):
    """Process a chunk of the data."""
    # Drop rows with missing Age or Sex
    chunk = chunk.dropna(subset=['Age', 'Sex']).copy()
    
    # Convert Age to numeric and filter
    chunk.loc[:, 'Age'] = pd.to_numeric(chunk['Age'], errors='coerce')
    chunk = chunk[(chunk['Age'] >= 18) & (chunk['Age'] <= 85)]
    
    # Map Age to Age Group
    chunk['Age Group'] = chunk['Age'].apply(
        lambda x: 'young adult' if 18 <= x <= 35 else 
                  'middle-aged adult' if 36 <= x <= 55 else 
                  'older adult'
    )
    
    # Convert reactions to lowercase and split
    chunk['Reactions'] = chunk['Reactions'].str.lower().str.split(', ')
    
    # Group by Drug Name, Age Group, and Sex
    grouped = chunk.groupby(['Drug Name', 'Age Group', 'Sex'])['Reactions']
    
    rows = []
    for (drug, age_group, sex), reactions_list in grouped:
        # Flatten the list of reactions
        all_reactions = [reaction for sublist in reactions_list for reaction in sublist]
        
        # Count the reactions
        reaction_counts = Counter(all_reactions)
        
        # Create a row with the reaction counts
        row = {'Drug Name': drug, 'Age Group': age_group, 'Sex': sex}
        for reaction in unique_reactions:
            row[reaction] = reaction_counts.get(reaction, 0)
        
        # Append the row to the list
        rows.append(row)
    
    # Return processed rows as a DataFrame
    return pd.DataFrame(rows)

def process_file(file_path):
    """Process a single file in chunks and write the results."""
    chunks = pd.read_csv(file_path, chunksize=5000)
    for chunk in chunks:
        # Process the chunk
        processed_chunk = process_chunk(chunk)
        
        # Append processed data to the output file
        processed_chunk.to_csv(output_file, mode='a', index=False, header=False)

if __name__ == '__main__':
    # Get all CSV files, excluding 'unique_reactions.csv'
    file_paths = [file for file in glob('*.csv') if file != 'unique_reactions.csv']
    
    # Parallel processing using ProcessPoolExecutor
    with ProcessPoolExecutor(max_workers=4) as executor:  # Adjust max_workers based on your system
        executor.map(process_file, file_paths)
    
    print(f"Processing complete. Output saved to '{output_file}'.")

#echo "Rows: $(($(wc -l < processed_2005_adverse_drug_reactions_extended.csv) - 1)), Columns: $(head -n 1 processed_2005_adverse_drug_reactions_extended.csv | awk -F',' '{print NF}')"
