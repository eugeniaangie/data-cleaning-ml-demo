import pandas as pd
import numpy as np
from fuzzywuzzy import fuzz
from geopy.distance import geodesic
import hashlib
import psycopg2
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class DuplicateChecker:
    def __init__(self, db_config=None):
        """Initialize duplicate checker with database connection"""
        if db_config:
            self.engine = create_engine(
                f"postgresql://{db_config['user']}:{db_config['password']}@"
                f"{db_config['host']}:{db_config['port']}/{db_config['database']}"
            )
        else:
            # Default config untuk demo
            self.engine = None
        
        # Threshold untuk duplicate detection
        self.name_similarity_threshold = 85  # 85% similarity
        self.distance_threshold = 50  # 50 meter
        
    def calculate_similarity(self, name1, name2):
        """Hitung similarity score antara 2 nama"""
        return fuzz.ratio(name1.lower(), name2.lower())
    
    def calculate_distance(self, lat1, lon1, lat2, lon2):
        """Hitung jarak dalam meter antara 2 koordinat"""
        point1 = (lat1, lon1)
        point2 = (lat2, lon2)
        return geodesic(point1, point2).meters
    
    def generate_data_hash(self, name, lat, lon):
        """Generate hash untuk deteksi duplicate"""
        data_string = f"{name.lower()}_{lat}_{lon}"
        return hashlib.md5(data_string.encode()).hexdigest()
    
    def detect_duplicates(self, df):
        """Deteksi duplicate dalam dataframe"""
        duplicates = []
        total_comparisons = len(df) * (len(df) - 1) // 2
        print(f"🔍 Checking {total_comparisons} combinations...")
        
        for i in range(len(df)):
            for j in range(i + 1, len(df)):
                row1 = df.iloc[i]
                row2 = df.iloc[j]
                
                # Hitung similarity nama
                name_similarity = self.calculate_similarity(
                    row1['name'], row2['name']
                )
                
                # Hitung jarak koordinat
                distance = self.calculate_distance(
                    row1['latitude'], row1['longitude'],
                    row2['latitude'], row2['longitude']
                )
                
                # Cek apakah duplicate
                is_duplicate = (
                    name_similarity >= self.name_similarity_threshold and
                    distance <= self.distance_threshold
                )
                
                if is_duplicate:
                    duplicate_info = {
                        'location_1': {
                            'id': row1.get('id', i),
                            'name': row1['name'],
                            'coordinates': (row1['latitude'], row1['longitude'])
                        },
                        'location_2': {
                            'id': row2.get('id', j),
                            'name': row2['name'],
                            'coordinates': (row2['latitude'], row2['longitude'])
                        },
                        'similarity_score': name_similarity,
                        'distance_meters': round(distance, 2)
                    }
                    duplicates.append(duplicate_info)
                    
                    print(f"🚨 DUPLICATE FOUND:")
                    print(f"   {row1['name']} vs {row2['name']}")
                    print(f"   Similarity: {name_similarity}%")
                    print(f"   Distance: {distance:.1f}m")
                    print()
        
        return duplicates
    
    def clean_duplicates(self, df, duplicates):
        """Hapus duplicate dari dataframe"""
        print(f"🧹 Cleaning {len(duplicates)} duplicates...")
        
        # Simpan ID yang akan dihapus
        ids_to_remove = set()
        
        for dup in duplicates:
            # Pilih yang akan dihapus (biasanya yang kedua)
            id_to_remove = dup['location_2']['id']
            ids_to_remove.add(id_to_remove)
            
            print(f"❌ Removing: {dup['location_2']['name']}")
        
        # Filter dataframe
        cleaned_df = df[~df.index.isin(ids_to_remove)].copy()
        
        print(f"✅ Cleaned data: {len(df)} → {len(cleaned_df)} locations")
        return cleaned_df
    
    def save_to_database(self, df, table_name='locations'):
        """Save cleaned data ke database"""
        if self.engine is None:
            print("⚠️  No database connection. Saving to CSV instead...")
            df.to_csv('cleaned_coffee_shops.csv', index=False)
            return
        
        try:
            # Generate data hash untuk setiap row
            df['data_hash'] = df.apply(
                lambda row: self.generate_data_hash(
                    row['name'], row['latitude'], row['longitude']
                ), axis=1
            )
            
            # Clear existing data dan insert cleaned data
            with self.engine.connect() as conn:
                # Delete existing data
                conn.execute(f"DELETE FROM {table_name}")
                conn.commit()
                
                # Insert cleaned data
                df.to_sql(table_name, self.engine, if_exists='append', index=False)
                print(f"💾 Replaced database with {len(df)} cleaned locations")
            
        except Exception as e:
            print(f"❌ Database error: {e}")
            df.to_csv('cleaned_coffee_shops.csv', index=False)
            print("💾 Saved to CSV instead")

def main():
    """Main function untuk demo"""
    print("🚀 Coffee Shop Duplicate Detection Demo")
    print("=" * 50)
    
    # Load sample data
    try:
        df = pd.read_csv('coffee_shop_data.csv')
        print(f"📊 Loaded {len(df)} coffee shops")
    except FileNotFoundError:
        print("❌ File coffee_shop_data.csv not found!")
        print("💡 Run generate_sample_data.py first")
        return
    
    # Initialize checker
    checker = DuplicateChecker()
    
    # Detect duplicates
    duplicates = checker.detect_duplicates(df)
    
    if duplicates:
        print(f"\n🎯 Found {len(duplicates)} potential duplicates")
        print("🧹 Auto-cleaning duplicates...")
        
        # clean duplicates
        cleaned_df = checker.clean_duplicates(df, duplicates)
        checker.save_to_database(cleaned_df)
    else:
        print("✅ No duplicates found!")
    
    print("\n🎉 Duplicate detection completed!")

if __name__ == "__main__":
    main()
