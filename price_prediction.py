import pandas as pd
import numpy as np
from sklearn.neighbors import KNeighborsRegressor
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_absolute_error, r2_score
import joblib
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class PricePredictor:
    def __init__(self, model_path='./models/'):
        """Initialize price predictor"""
        self.model = None
        self.scaler = StandardScaler()
        self.feature_columns = ['latitude', 'longitude', 'area_sqm', 'rating', 'location_followers', 'engagement_rate']
        self.model_path = model_path
        
        # Create models directory if not exists
        os.makedirs(model_path, exist_ok=True)
    
    def get_data_from_database(self):
        """Ambil data dari database dengan join table"""
        try:
            # Database config
            db_config = {
                'host': os.getenv('DB_HOST', 'localhost'),
                'port': os.getenv('DB_PORT', '5432'),
                'database': os.getenv('DB_NAME', 'coffee_shop_db'),
                'user': os.getenv('DB_USER', 'postgres'),
                'password': os.getenv('DB_PASSWORD', 'postgres')
            }
            
            engine = create_engine(
                f"postgresql://{db_config['user']}:{db_config['password']}@"
                f"{db_config['host']}:{db_config['port']}/{db_config['database']}"
            )
            
            # Join locations dengan prices
            query = """
            SELECT l.id, l.name, l.latitude, l.longitude, l.area_sqm, l.rating, 
                   l.followers as location_followers, p.price_per_sqm, p.monthly_rent, 
                   s.followers as social_followers, s.engagement_rate
            FROM locations l
            LEFT JOIN prices p ON l.id = p.location_id
            LEFT JOIN social_metrics s ON l.id = s.location_id
            """
            
            df = pd.read_sql(query, engine)
            print(f"📊 Loaded {len(df)} records from database")
            return df
            
        except Exception as e:
            print(f"⚠️  Database error: {e}")
            return None
    
    def prepare_data(self, df):
        """Prepare data untuk training"""
        print("🔧 Preparing data...")
        
        # Generate price_per_sqm if not exists
        if 'price_per_sqm' not in df.columns or df['price_per_sqm'].isna().all():
            print("💰 Generating price_per_sqm data...")
            df['price_per_sqm'] = np.random.randint(150000, 400000, len(df))
        
        # Select features dan target
        X = df[self.feature_columns].values
        y = df['price_per_sqm'].values
        
        # Handle missing values
        X = np.nan_to_num(X, nan=0)
        y = np.nan_to_num(y, nan=0)
        
        print(f"📊 Features: {X.shape}")
        print(f"📊 Target: {y.shape}")
        
        return X, y
    
    def train_model(self, X, y, test_size=0.2, n_neighbors=5):
        """Train KNN model"""
        print("🤖 Training KNN model...")
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=test_size, random_state=42
        )
        
        # Scale features
        X_train_scaled = self.scaler.fit_transform(X_train)
        X_test_scaled = self.scaler.transform(X_test)
        
        # Train model
        self.model = KNeighborsRegressor(n_neighbors=n_neighbors)
        self.model.fit(X_train_scaled, y_train)
        
        # Evaluate model
        y_pred = self.model.predict(X_test_scaled)
        mae = mean_absolute_error(y_test, y_pred)
        r2 = r2_score(y_test, y_pred)
        
        print(f"📈 Model Performance:")
        print(f"   MAE: Rp {mae:,.0f}/m²")
        print(f"   R²: {r2:.3f}")
        
        return self.model
    
    def predict_price(self, latitude, longitude, area_sqm, rating, followers, engagement_rate=5.0):
        """Predict price untuk lokasi baru"""
        if self.model is None:
            raise ValueError("Model belum di-train! Jalankan train_model() dulu.")
        
        # Prepare input data (6 features)
        new_data = np.array([[latitude, longitude, area_sqm, rating, followers, engagement_rate]])
        new_data_scaled = self.scaler.transform(new_data)
        
        # Predict
        predicted_price = self.model.predict(new_data_scaled)[0]
        
        return predicted_price
    
    def find_similar_locations(self, latitude, longitude, area_sqm, rating, followers, engagement_rate=5.0, n_neighbors=5):
        """Cari lokasi yang mirip"""
        if self.model is None:
            raise ValueError("Model belum di-train!")
        
        # Prepare input data (6 features)
        new_data = np.array([[latitude, longitude, area_sqm, rating, followers, engagement_rate]])
        new_data_scaled = self.scaler.transform(new_data)
        
        # Find nearest neighbors
        distances, indices = self.model.kneighbors(new_data_scaled)
        
        return distances[0], indices[0]
    
    def save_model(self, filename='knn_price_model.pkl'):
        """Save trained model"""
        if self.model is None:
            print("❌ No model to save!")
            return
        
        model_file = os.path.join(self.model_path, filename)
        scaler_file = os.path.join(self.model_path, 'scaler.pkl')
        
        joblib.dump(self.model, model_file)
        joblib.dump(self.scaler, scaler_file)
        
        print(f"💾 Model saved to {model_file}")
        print(f"💾 Scaler saved to {scaler_file}")
    
    def load_model(self, filename='knn_price_model.pkl'):
        """Load trained model"""
        model_file = os.path.join(self.model_path, filename)
        scaler_file = os.path.join(self.model_path, 'scaler.pkl')
        
        if not os.path.exists(model_file):
            print("❌ Model file not found!")
            return False
        
        self.model = joblib.load(model_file)
        self.scaler = joblib.load(scaler_file)
        
        print(f"📂 Model loaded from {model_file}")
        return True

def demo_prediction():
    """Demo prediction untuk beberapa lokasi"""
    print("🎯 Price Prediction Demo")
    print("=" * 40)
    
    # Initialize predictor
    predictor = PricePredictor()
    
    # Load data dari database atau CSV
    df = predictor.get_data_from_database()
    
    if df is None or len(df) == 0:
        print("⚠️  No data from database, trying CSV...")
        try:
            df = pd.read_csv('coffee_shop_data.csv')
            print(f"📊 Loaded {len(df)} coffee shops from CSV")
        except FileNotFoundError:
            print("❌ No data found! Run generate_sample_data.py first")
            return
    
    # Prepare data
    X, y = predictor.prepare_data(df)
    
    # Train model
    predictor.train_model(X, y)
    
    # Demo predictions
    demo_locations = [
        {
            'name': 'Sudirman CBD',
            'lat': -6.2088, 'lon': 106.8456,
            'area': 100, 'rating': 4.5, 'followers': 15000, 'engagement': 5.5
        },
        {
            'name': 'Kemang',
            'lat': -6.2600, 'lon': 106.8100,
            'area': 80, 'rating': 4.2, 'followers': 12000, 'engagement': 4.8
        },
        {
            'name': 'Menteng',
            'lat': -6.2000, 'lon': 106.8300,
            'area': 120, 'rating': 4.7, 'followers': 20000, 'engagement': 6.2
        }
    ]
    
    print("\n🔮 PREDICTIONS:")
    print("-" * 50)
    
    for loc in demo_locations:
        predicted_price = predictor.predict_price(
            loc['lat'], loc['lon'], loc['area'], 
            loc['rating'], loc['followers'], loc['engagement']
        )
        
        total_rent = predicted_price * loc['area']
        
        print(f"📍 {loc['name']}")
        print(f"   Price/m²: Rp {predicted_price:,.0f}")
        print(f"   Total rent: Rp {total_rent:,.0f}/month")
        print()
    
    # Find similar locations
    print("🔍 SIMILAR LOCATIONS:")
    print("-" * 30)
    
    target_loc = demo_locations[0]  # Sudirman CBD
    distances, indices = predictor.find_similar_locations(
        target_loc['lat'], target_loc['lon'], target_loc['area'],
        target_loc['rating'], target_loc['followers'], target_loc['engagement']
    )
    
    print(f"Most similar to {target_loc['name']}:")
    for i, (dist, idx) in enumerate(zip(distances, indices)):
        similar_shop = df.iloc[idx]
        print(f"{i+1}. {similar_shop['name']} - "
              f"Rp {similar_shop['price_per_sqm']:,.0f}/m²")
    
    # Save model
    predictor.save_model()

def main():
    """Main function"""
    demo_prediction()

if __name__ == "__main__":
    main()
