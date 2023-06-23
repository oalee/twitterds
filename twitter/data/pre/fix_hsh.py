# import pyarrow as pa
# import pyarrow.parquet as pq

# # Read the Parquet file
# table = pq.read_table("/run/media/al/data/td/users/_AAN90/tweets.parquet")

# # Find the 'hashtags' field in the schema
# hashtags_field = table.schema.field('hashtags')

# # If the 'hashtags' field isn't the correct type, replace it
# if not hashtags_field.type.equals(pa.list_(pa.string())):
#     # Create a new array of nulls with the correct type
#     num_rows = len(table)
#     new_array = pa.array([None] * num_rows, type=pa.list_(pa.string()))
    
#     # Create a list of columns, excluding 'hashtags'
#     columns = [field.name for field in table.schema if field.name != 'hashtags']
    
#     # Create a new table without the 'hashtags' column
#     new_table = table.select(columns)
    
#     # Add the new 'hashtags' column
#     new_table = new_table.append_column('hashtags', new_array)

# # Save the table back to a Parquet file
# pq.write_table(new_table, "/run/media/al/data/td/users/_AAN90/tweets.parquet")


import os
import multiprocessing as mp
import pyarrow as pa
import pyarrow.parquet as pq

def process_file(filepath):
    # Read the Parquet file
    table = pq.read_table(filepath)

    # Find the 'hashtags' field in the schema
    hashtags_field = table.schema.field('hashtags')

    # If the 'hashtags' field isn't the correct type, replace it
    if not hashtags_field.type.equals(pa.list_(pa.string())):
        # Create a new array of nulls with the correct type
        num_rows = len(table)
        new_array = pa.array([None] * num_rows, type=pa.list_(pa.string()))
        
        # Create a list of columns, excluding 'hashtags'
        columns = [field.name for field in table.schema if field.name != 'hashtags']
        
        # Create a new table without the 'hashtags' column
        new_table = table.select(columns)
        
        # Add the new 'hashtags' column
        new_table = new_table.append_column('hashtags', new_array)

        # Save the table back to a Parquet file
        fixed_filepath = filepath#.replace('.parquet', '_fixed.parquet')
        pq.write_table(new_table, fixed_filepath)
        print(f'Fixed file saved to {fixed_filepath}')
    else:
        pass
        # print(f'No fixes needed for {filepath}')

def process_directory(user_dir):
    for root, dirs, files in os.walk(user_dir):
        for file in files:
            if file.endswith(".parquet"):
                process_file(os.path.join(root, file))

def main():
    root_dir = '/run/media/al/data/td/users'
    user_dirs = [os.path.join(root_dir, user_dir) for user_dir in os.listdir(root_dir)]

    # Use multiprocessing to process each user directory in parallel
    with mp.Pool(processes=mp.cpu_count()) as pool:
        pool.map(process_directory, user_dirs)

if __name__ == "__main__":
    main()
