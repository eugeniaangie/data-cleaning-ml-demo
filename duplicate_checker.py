import pandas as pd
import numpy as np
from rapidfuzz import fuzz
from math import radians, sin, cos, sqrt, atan2
import hashlib
import psycopg2
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
import time

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
            # Try to get database config from environment
            db_config = {
                'host': os.getenv('DB_HOST', 'localhost'),
                'port': os.getenv('DB_PORT', '5432'),
                'database': os.getenv('DB_NAME', 'coffee_shop_db'),
                'user': os.getenv('DB_USER', 'postgres'),
                'password': os.getenv('DB_PASSWORD', 'postgres')
            }
            
            try:
                self.engine = create_engine(
                    f"postgresql://{db_config['user']}:{db_config['password']}@"
                    f"{db_config['host']}:{db_config['port']}/{db_config['database']}"
                )
                print("✅ Database connection established")
            except Exception as e:
                print(f"⚠️  Database connection failed: {e}")
                self.engine = None
        
        # Threshold untuk duplicate detection
        self.name_similarity_threshold = 85  # 85% similarity
        self.distance_threshold = 50  # 50 meter
        
    def calculate_similarity(self, name1, name2):
        """Hitung similarity score antara 2 nama menggunakan RapidFuzz"""
        return fuzz.ratio(name1.lower(), name2.lower())
    
    def calculate_distance(self, lat1, lon1, lat2, lon2):
        """Hitung jarak dalam meter menggunakan Haversine formula"""
        R = 6371000  # Earth radius in meters
        
        lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        
        a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
        c = 2 * atan2(sqrt(a), sqrt(1-a))
        distance = R * c
        
        return distance
    
    def generate_data_hash(self, name, lat, lon):
        """Generate hash untuk deteksi duplicate"""
        data_string = f"{name.lower()}_{lat}_{lon}"
        return hashlib.md5(data_string.encode()).hexdigest()
    
    def detect_duplicates(self, df):
        """Deteksi duplicate dalam dataframe"""
        duplicates = []
        total_comparisons = len(df) * (len(df) - 1) // 2
        print(f"🔍 Checking {total_comparisons} combinations...")
        
        start_time = time.time()
        comparisons = 0
        
        for i in range(len(df)):
            for j in range(i + 1, len(df)):
                comparisons += 1
                row1 = df.iloc[i]
                row2 = df.iloc[j]
                
                # Hitung similarity nama (RapidFuzz - lebih cepat)
                name_similarity = self.calculate_similarity(
                    row1['name'], row2['name']
                )
                
                # Hitung jarak koordinat (Haversine - lebih cepat)
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
                
                # Progress indicator untuk dataset besar
                if comparisons % 100 == 0:
                    elapsed = time.time() - start_time
                    rate = comparisons / elapsed if elapsed > 0 else 0
                    print(f"⏳ Progress: {comparisons}/{total_comparisons} ({rate:.0f} comparisons/sec)")
        
        end_time = time.time()
        print(f"⏱️  Analysis completed in {end_time - start_time:.2f} seconds")
        print(f"🔢 Total comparisons: {comparisons}")
        
        return duplicates
    
    def log_duplicates_to_db(self, duplicates):
        """Log duplicate detection results to database"""
        if self.engine is None or not duplicates:
            return
            
        try:
            with self.engine.connect() as conn:
                from sqlalchemy import text
                
                # Insert duplicate log records
                for dup in duplicates:
                    conn.execute(text("""
                        INSERT INTO duplicate_log 
                        (location_id_1, location_id_2, similarity_score, distance_meters, action_taken)
                        VALUES (:loc1, :loc2, :similarity, :distance, :action)
                    """), {
                        'loc1': int(dup['location_1']['id']),
                        'loc2': int(dup['location_2']['id']),
                        'similarity': float(dup['similarity_score']),
                        'distance': float(dup['distance_meters']),
                        'action': 'removed_duplicate'
                    })
                
                conn.commit()
                print(f"📝 Logged {len(duplicates)} duplicate detections to database")
                
        except Exception as e:
            print(f"⚠️  Failed to log duplicates: {e}")
    
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
    
    def regenerate_related_data(self, df, conn):
        """Regenerate data untuk table prices dan social_metrics"""
        import random
        from datetime import datetime
        from sqlalchemy import text
        
        print("🔄 Regenerating related data...")
        
        # Generate prices data
        prices_data = []
        for _, row in df.iterrows():
            price_per_sqm = random.randint(150000, 400000)
            monthly_rent = price_per_sqm * row['area_sqm']
            prices_data.append({
                'location_id': row['id'],
                'monthly_rent': monthly_rent,
                'price_per_sqm': price_per_sqm,
                'date_recorded': datetime.now().date(),
                'source': 'regenerated'
            })
        
        prices_df = pd.DataFrame(prices_data)
        prices_df.to_sql('prices', self.engine, if_exists='append', index=False)
        print(f"💾 Inserted {len(prices_df)} price records")
        
        # Generate social metrics data
        social_data = []
        for _, row in df.iterrows():
            social_data.append({
                'location_id': row['id'],
                'platform': 'instagram',
                'followers': row['followers'],
                'engagement_rate': round(random.uniform(2.0, 8.0), 2),
                'last_updated': datetime.now()
            })
        
        social_df = pd.DataFrame(social_data)
        social_df.to_sql('social_metrics', self.engine, if_exists='append', index=False)
        print(f"💾 Inserted {len(social_df)} social metrics records")
    
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
                from sqlalchemy import text
                
                # Delete existing data (handle foreign key constraints)
                conn.execute(text("DELETE FROM duplicate_log"))
                conn.execute(text("DELETE FROM prices"))
                conn.execute(text("DELETE FROM social_metrics"))
                conn.execute(text(f"DELETE FROM {table_name}"))
                conn.commit()
                
                # Insert cleaned data
                df.to_sql(table_name, self.engine, if_exists='append', index=False)
                print(f"💾 Replaced database with {len(df)} cleaned locations")
                
                # Regenerate data untuk table lain
                self.regenerate_related_data(df, conn)
            
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
        
        # Log duplicates to database
        checker.log_duplicates_to_db(duplicates)
        
        # clean duplicates
        cleaned_df = checker.clean_duplicates(df, duplicates)
        checker.save_to_database(cleaned_df)
    else:
        print("✅ No duplicates found!")
        print("💾 Saving original data to database...")
        checker.save_to_database(df)
    
    print("\n🎉 Duplicate detection completed!")

if __name__ == "__main__":
    main()
