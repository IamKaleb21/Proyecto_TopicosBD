#!/usr/bin/env python3
"""
Script to reduce the number of records in hotel_bookings.csv to 15,000
and save it as a new file. Records are selected randomly ensuring at least
32% of cancellations in the final dataset.
"""

import pandas as pd
import os
from pathlib import Path
import numpy as np

def reduce_hotel_bookings(input_file='hotel_bookings.csv', 
                         output_file='hotel_bookings_reduced.csv', 
                         target_records=15638,
                         min_cancellation_rate=0.3265):
    """
    Reduce the number of records in the hotel bookings CSV file by randomly sampling,
    ensuring a minimum percentage of cancellations.
    
    Args:
        input_file (str): Path to the input CSV file
        output_file (str): Path to the output CSV file
        target_records (int): Number of records to keep
        min_cancellation_rate (float): Minimum proportion of cancellations desired (0-1)
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
        
        # Calculate the number of cancelled and non-cancelled records needed
        cancelled_records = int(np.ceil(target_records * min_cancellation_rate))
        non_cancelled_records = target_records - cancelled_records
        
        # Split the dataframe into cancelled and non-cancelled
        df_cancelled = df[df['is_canceled'] == 1]
        df_non_cancelled = df[df['is_canceled'] == 0]
        
        print(f"\nOriginal cancellation rate: {(len(df_cancelled) / len(df)) * 100:.1f}%")
        
        # Sample from each group
        df_cancelled_sample = df_cancelled.sample(n=cancelled_records, random_state=42)
        df_non_cancelled_sample = df_non_cancelled.sample(n=non_cancelled_records, random_state=42)
        
        # Combine the samples and shuffle
        df_reduced = pd.concat([df_cancelled_sample, df_non_cancelled_sample])
        df_reduced = df_reduced.sample(frac=1, random_state=42)  # Shuffle the combined dataset
        
        # Calculate final cancellation rate
        final_cancel_rate = (len(df_reduced[df_reduced['is_canceled'] == 1]) / len(df_reduced)) * 100
        print(f"Final cancellation rate: {final_cancel_rate:.1f}%")
        
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