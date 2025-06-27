#!/usr/bin/env python3
"""
Script to reduce the number of records in hotel_bookings.csv to 15,000
and save it as a new file.
"""

import pandas as pd
import os
from pathlib import Path

def reduce_hotel_bookings(input_file='hotel_bookings.csv', 
                         output_file='hotel_bookings_reduced.csv', 
                         target_records=15638):
    """
    Reduce the number of records in the hotel bookings CSV file.
    
    Args:
        input_file (str): Path to the input CSV file
        output_file (str): Path to the output CSV file
        target_records (int): Number of records to keep
    """
    
    print(f"Reading {input_file}...")
    
    # Check if input file exists
    if not os.path.exists(input_file):
        print(f"Error: {input_file} not found!")
        return False
    
    try:
        # Read the CSV file
        df = pd.read_csv(input_file)
        
        original_count = len(df)
        print(f"Original file has {original_count:,} records")
        
        # Check if we need to reduce the data
        if original_count <= target_records:
            print(f"File already has {original_count:,} records or fewer. No reduction needed.")
            print(f"Copying file to {output_file}...")
            df.to_csv(output_file, index=False)
            print(f"File saved as {output_file}")
            return True
        
        # Reduce to target number of records
        print(f"Reducing to {target_records:,} records...")
        df_reduced = df.head(target_records)
        
        # Save the reduced dataset
        print(f"Saving reduced dataset to {output_file}...")
        df_reduced.to_csv(output_file, index=False)
        
        print(f"Success! Reduced from {original_count:,} to {len(df_reduced):,} records")
        print(f"New file saved as: {output_file}")
        
        # Show file size comparison
        original_size = os.path.getsize(input_file) / (1024 * 1024)  # MB
        new_size = os.path.getsize(output_file) / (1024 * 1024)  # MB
        
        print(f"Original file size: {original_size:.2f} MB")
        print(f"New file size: {new_size:.2f} MB")
        print(f"Size reduction: {((original_size - new_size) / original_size * 100):.1f}%")
        
        return True
        
    except Exception as e:
        print(f"Error processing file: {str(e)}")
        return False

def main():
    """Main function to execute the reduction process."""
    print("=" * 60)
    print("Hotel Bookings CSV Reduction Script")
    print("=" * 60)
    
    # Execute the reduction
    success = reduce_hotel_bookings()
    
    if success:
        print("\n✅ Process completed successfully!")
    else:
        print("\n❌ Process failed!")
    
    print("=" * 60)

if __name__ == "__main__":
    main() 