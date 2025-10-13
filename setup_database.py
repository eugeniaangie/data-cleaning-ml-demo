import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def create_database():
    """Create database"""
    try:
        # Connect to PostgreSQL server
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST', 'localhost'),
            port=os.getenv('DB_PORT', '5432'),
            user=os.getenv('DB_USER', 'myuser'),
            password=os.getenv('DB_PASSWORD', 'postgres')
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        # Check if database exists
        db_name = os.getenv('DB_NAME', 'coffee_shop_db')
        cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{db_name}'")
        
        if cursor.fetchone():
            print(f"✅ Database '{db_name}' already exists")
        else:
            # Create database
            cursor.execute(f"CREATE DATABASE {db_name}")
            print(f"✅ Database '{db_name}' created successfully")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Database creation error: {e}")
        print("💡 Make sure PostgreSQL is running and credentials are correct")

def create_tables():
    """Create tables dari schema file"""
    try:
        # Read schema file
        with open('database_schema.sql', 'r') as f:
            schema_sql = f.read()
        
        # Connect to database
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST', 'localhost'),
            port=os.getenv('DB_PORT', '5432'),
            user=os.getenv('DB_USER', 'myuser'),
            password=os.getenv('DB_PASSWORD', 'postgres'),
            database=os.getenv('DB_NAME', 'coffee_shop_db')
        )
        cursor = conn.cursor()
        
        # Execute schema
        cursor.execute(schema_sql)
        conn.commit()
        
        print("✅ Tables created successfully")
        
        # Show created tables
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
        """)
        tables = cursor.fetchall()
        
        print("📋 Created tables:")
        for table in tables:
            print(f"   - {table[0]}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Table creation error: {e}")

def test_connection():
    """Test database connection"""
    try:
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST', 'localhost'),
            port=os.getenv('DB_PORT', '5432'),
            user=os.getenv('DB_USER', 'postgres'),
            password=os.getenv('DB_PASSWORD', 'password'),
            database=os.getenv('DB_NAME', 'coffee_shop_db')
        )
        
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()
        
        print(f"✅ Database connection successful!")
        print(f"📊 PostgreSQL version: {version[0]}")
        
        cursor.close()
        conn.close()
        
        return True
        
    except Exception as e:
        print(f"❌ Connection test failed: {e}")
        return False

def main():
    """Main setup function"""
    print("🏗️  Setting up Coffee Shop Database")
    print("=" * 40)
    
    # Check if .env file exists
    if not os.path.exists('.env'):
        print("⚠️  .env file not found!")
        print("💡 Copy env_example.txt to .env and update your database credentials")
        print("📝 Example:")
        print("   DB_HOST=localhost")
        print("   DB_PORT=5432")
        print("   DB_NAME=coffee_shop_db")
        print("   DB_USER=postgres")
        print("   DB_PASSWORD=your_password")
        return
    
    # 1. Create database
    print("\n1️⃣ Creating database...")
    create_database()
    
    # 2. Create tables
    print("\n2️⃣ Creating tables...")
    create_tables()
    
    # 3. Test connection
    print("\n3️⃣ Testing connection...")
    if test_connection():
        print("\n🎉 Database setup completed successfully!")
        print("💡 You can now run the data processing scripts")
    else:
        print("\n❌ Database setup failed!")
        print("💡 Check your PostgreSQL installation and credentials")

if __name__ == "__main__":
    main()
