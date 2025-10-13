import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

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
        
        # Generate realistic data
        shop_data = {
            'id': i + 1,
            'name': f"{chain} {area['name']}",
            'latitude': area['lat'] + lat_variation,
            'longitude': area['lon'] + lon_variation,
            'address': f"Jl. {area['name']} No. {random.randint(1, 200)}",
            'area_sqm': random.randint(50, 200),
            'rating': round(random.uniform(3.5, 5.0), 1),
            'followers': random.randint(5000, 50000),
            'price_per_sqm': random.randint(150000, 400000),
            'monthly_rent': 0,  # Will calculate later
            'created_at': datetime.now() - timedelta(days=random.randint(1, 365))
        }
        
        # Calculate monthly rent
        shop_data['monthly_rent'] = shop_data['price_per_sqm'] * shop_data['area_sqm']
        
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
            'price_per_sqm': random.randint(200000, 350000),
            'monthly_rent': 0,
            'created_at': datetime.now() - timedelta(days=random.randint(1, 365))
        }
        orig_data['monthly_rent'] = orig_data['price_per_sqm'] * orig_data['area_sqm']
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
            'price_per_sqm': orig_data['price_per_sqm'] + random.randint(-50000, 50000),
            'monthly_rent': 0,
            'created_at': datetime.now() - timedelta(days=random.randint(1, 365))
        }
        dup_data['monthly_rent'] = dup_data['price_per_sqm'] * dup_data['area_sqm']
        data.append(dup_data)
    
    # Convert to DataFrame
    df = pd.DataFrame(data)
    
    # Shuffle data
    df = df.sample(frac=1).reset_index(drop=True)
    
    # Reassign IDs
    df['id'] = range(1, len(df) + 1)
    
    return df

def main():
    """Generate dan save sample data"""
    print("🏗️  Generating sample coffee shop data...")
    
    # Generate data
    df = generate_coffee_shop_data(50)
    
    # Save to CSV
    df.to_csv('coffee_shop_data.csv', index=False)
    
    print(f"✅ Generated {len(df)} coffee shops")
    print(f"💾 Saved to coffee_shop_data.csv")
    
    # Show sample data
    print("\n📊 Sample Data:")
    print(df[['name', 'latitude', 'longitude', 'area_sqm', 'price_per_sqm']].head(10))
    
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
