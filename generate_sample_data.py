import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
import hashlib
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def generate_coffee_shop_data(n_shops=50):
    """Generate sample coffee shop data dengan beberapa duplicate"""
    
    # Popular coffee shop names di Jakarta
    coffee_chains = [
        "Starbucks", "Kopi Kenangan", "Janji Jiwa", "Fore Coffee",
        "Anomali Coffee", "Common Grounds", "Giyanti Coffee", "Tanamera Coffee"
    ]
    
    # Jakarta areas dengan koordinat
    jakarta_areas = [
        {"name": "Sudirman", "lat": -6.2088, "lon": 106.8456},
        {"name": "Thamrin", "lat": -6.1950, "lon": 106.8200},
        {"name": "Kemang", "lat": -6.2600, "lon": 106.8100},
        {"name": "Menteng", "lat": -6.2000, "lon": 106.8300},
        {"name": "Senayan", "lat": -6.2200, "lon": 106.8000},
        {"name": "Kuningan", "lat": -6.2400, "lon": 106.8300},
        {"name": "Blok M", "lat": -6.2400, "lon": 106.8000},
        {"name": "Pondok Indah", "lat": -6.2600, "lon": 106.7800}
    ]
    
    data = []
    
    # Generate normal data
    for i in range(n_shops - 10):  # Leave space for duplicates
        chain = random.choice(coffee_chains)
        area = random.choice(jakarta_areas)
        
        # Add some variation to coordinates (within 1km radius)
        lat_variation = random.uniform(-0.01, 0.01)
        lon_variation = random.uniform(-0.01, 0.01)
        
        # Generate realistic data (matching database schema)
        shop_data = {
            'id': i + 1,
            'name': f"{chain} {area['name']}",
            'latitude': area['lat'] + lat_variation,
            'longitude': area['lon'] + lon_variation,
            'address': f"Jl. {area['name']} No. {random.randint(1, 200)}",
            'area_sqm': random.randint(50, 200),
            'rating': round(random.uniform(3.5, 5.0), 1),
            'followers': random.randint(5000, 50000),
            'created_at': datetime.now() - timedelta(days=random.randint(1, 365))
        }
        
        # Generate price data
        price_per_sqm = random.randint(150000, 400000)
        monthly_rent = price_per_sqm * shop_data['area_sqm']
        
        # Generate social metrics
        engagement_rate = round(random.uniform(2.0, 8.0), 2)
        
        data.append(shop_data)
    
    # Generate intentional duplicates
    duplicate_pairs = [
        {
            'original': {'name': 'Kopi Kenangan Sudirman', 'lat': -6.2088, 'lon': 106.8456},
            'duplicate': {'name': 'Kopi Kenangan - Sudirman', 'lat': -6.2089, 'lon': 106.8457}
        },
        {
            'original': {'name': 'Janji Jiwa CBD', 'lat': -6.1950, 'lon': 106.8200},
            'duplicate': {'name': 'Janji Jiwa Central Business District', 'lat': -6.1951, 'lon': 106.8201}
        },
        {
            'original': {'name': 'Starbucks Kemang', 'lat': -6.2600, 'lon': 106.8100},
            'duplicate': {'name': 'Starbucks Kemang Raya', 'lat': -6.2601, 'lon': 106.8101}
        },
        {
            'original': {'name': 'Fore Coffee Menteng', 'lat': -6.2000, 'lon': 106.8300},
            'duplicate': {'name': 'Fore Coffee - Menteng', 'lat': -6.2001, 'lon': 106.8301}
        },
        {
            'original': {'name': 'Anomali Coffee Senayan', 'lat': -6.2200, 'lon': 106.8000},
            'duplicate': {'name': 'Anomali Coffee Senayan City', 'lat': -6.2201, 'lon': 106.8001}
        }
    ]
    
    # Add duplicates
    for i, pair in enumerate(duplicate_pairs):
        # Original
        orig_data = {
            'id': len(data) + 1,
            'name': pair['original']['name'],
            'latitude': pair['original']['lat'],
            'longitude': pair['original']['lon'],
            'address': f"Jl. {random.choice(['Sudirman', 'Thamrin', 'Kemang', 'Menteng'])} No. {random.randint(1, 200)}",
            'area_sqm': random.randint(80, 150),
            'rating': round(random.uniform(4.0, 5.0), 1),
            'followers': random.randint(10000, 30000),
            'created_at': datetime.now() - timedelta(days=random.randint(1, 365))
        }
        data.append(orig_data)
        
        # Duplicate
        dup_data = {
            'id': len(data) + 1,
            'name': pair['duplicate']['name'],
            'latitude': pair['duplicate']['lat'],
            'longitude': pair['duplicate']['lon'],
            'address': f"Jl. {random.choice(['Sudirman', 'Thamrin', 'Kemang', 'Menteng'])} No. {random.randint(1, 200)}",
            'area_sqm': orig_data['area_sqm'] + random.randint(-10, 10),  # Slightly different
            'rating': orig_data['rating'] + random.uniform(-0.2, 0.2),
            'followers': orig_data['followers'] + random.randint(-5000, 5000),
            'created_at': datetime.now() - timedelta(days=random.randint(1, 365))
        }
        data.append(dup_data)
    
    # Convert to DataFrame
    df = pd.DataFrame(data)
    
    # Shuffle data
    df = df.sample(frac=1).reset_index(drop=True)
    
    # Reassign IDs
    df['id'] = range(1, len(df) + 1)
    
    return df

def save_to_database(df, table_name='locations'):
    """Save generated data to database"""
    # Try to get database config from environment
    db_config = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': os.getenv('DB_PORT', '5432'),
        'database': os.getenv('DB_NAME', 'coffee_shop_db'),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD', 'password')
    }
    
    try:
        # Create database connection
        engine = create_engine(
            f"postgresql://{db_config['user']}:{db_config['password']}@"
            f"{db_config['host']}:{db_config['port']}/{db_config['database']}"
        )
        
        # Generate data hash untuk setiap row
        df['data_hash'] = df.apply(
            lambda row: hashlib.md5(
                f"{row['name'].lower()}_{row['latitude']}_{row['longitude']}".encode()
            ).hexdigest(), axis=1
        )
        
        # Clear existing data dan insert new data
        with engine.connect() as conn:
            from sqlalchemy import text
            
            # Delete existing data from all tables
            conn.execute(text("DELETE FROM prices"))
            conn.execute(text("DELETE FROM social_metrics"))
            conn.execute(text("DELETE FROM duplicate_log"))
            conn.execute(text(f"DELETE FROM {table_name}"))
            conn.commit()
            
            # Insert locations
            df.to_sql(table_name, engine, if_exists='append', index=False)
            print(f"💾 Inserted {len(df)} locations to database")
            
            # Insert prices data
            prices_data = []
            for _, row in df.iterrows():
                price_per_sqm = random.randint(150000, 400000)
                monthly_rent = price_per_sqm * row['area_sqm']
                prices_data.append({
                    'location_id': row['id'],
                    'monthly_rent': monthly_rent,
                    'price_per_sqm': price_per_sqm,
                    'date_recorded': datetime.now().date(),
                    'source': 'generated'
                })
            
            prices_df = pd.DataFrame(prices_data)
            prices_df.to_sql('prices', engine, if_exists='append', index=False)
            print(f"💾 Inserted {len(prices_df)} price records")
            
            # Insert social metrics data
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
            social_df.to_sql('social_metrics', engine, if_exists='append', index=False)
            print(f"💾 Inserted {len(social_df)} social metrics records")
            
        return True
        
    except Exception as e:
        print(f"❌ Database error: {e}")
        print("💡 Make sure database is running and configured properly")
        return False

def main():
    """Generate dan save sample data"""
    print("🏗️  Generating sample coffee shop data...")
    
    # Generate data
    df = generate_coffee_shop_data(50)
    
    # Save to CSV
    df.to_csv('coffee_shop_data.csv', index=False)
    
    print(f"✅ Generated {len(df)} coffee shops")
    print(f"💾 Saved to coffee_shop_data.csv")
    
    # Try to save to database
    print("\n🗄️  Attempting to save to database...")
    db_success = save_to_database(df)
    
    if not db_success:
        print("⚠️  Database not available, data saved to CSV only")
    
    # Show sample data
    print("\n📊 Sample Data:")
    print(df[['name', 'latitude', 'longitude', 'area_sqm', 'rating']].head(10))
    
    # Show potential duplicates
    print("\n🔍 Potential Duplicates (for testing):")
    duplicate_names = [
        'Kopi Kenangan Sudirman', 'Kopi Kenangan - Sudirman',
        'Janji Jiwa CBD', 'Janji Jiwa Central Business District',
        'Starbucks Kemang', 'Starbucks Kemang Raya'
    ]
    
    for name in duplicate_names:
        if name in df['name'].values:
            shop = df[df['name'] == name].iloc[0]
            print(f"  {shop['name']} - {shop['latitude']:.4f}, {shop['longitude']:.4f}")
    
    print("\n🎉 Sample data generation completed!")
    print("💡 Now you can run duplicate_checker.py to test duplicate detection")

if __name__ == "__main__":
    main()
