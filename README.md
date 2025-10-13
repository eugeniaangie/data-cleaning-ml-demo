# ☕ Coffee Shop Data Management & ML Demo

Complete project for data cleaning & machine learning training with coffee shop case study in Jakarta.

## 🎯 Main Features

- **Duplicate Detection**: Detect & remove duplicate data using fuzzy matching + GPS distance
- **Database Management**: PostgreSQL with proper schema design

## 🚀 Quick Start

### 1. Setup Environment

```bash
# Clone or download project
cd data-cleaning-ml-demo

# Setup Python virtual environment
python -m venv env
source env/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Setup Database

```bash
# Install PostgreSQL
# macOS: brew install postgresql

# Setup database
python setup_database.py
```

### 3. Generate Sample Data

```bash
# Generate sample coffee shop data
python generate_sample_data.py
```

### 4. Run Demo Scripts

```bash
# Detect duplicates
python duplicate_checker.py

```

## 📁 Project Structure

```
data-cleaning-ml-demo/
├── 📊 Data & Scripts
│   ├── generate_sample_data.py      # Generate sample data
│   ├── duplicate_checker.py         # Duplicate detection
│
├── 🗄️ Database
│   ├── database_schema.sql         # PostgreSQL schema
│   └── env_example.txt             # Environment variables
│
│
├── 📚 Documentation
│   ├── README.md                   # This file
│   └── requirements.txt            # Python dependencies
│
└── 📄 Generated Files
    ├── coffee_shop_data.csv         # Sample data
    ├── cleaned_coffee_shops.csv    # Cleaned data
```

## 🛠️ Detailed Setup

### Database Configuration

1. **Install PostgreSQL**:
   ```bash
   # macOS
   brew install postgresql
   brew services start postgresql
   
   # Ubuntu/Debian
   sudo apt update
   sudo apt install postgresql postgresql-contrib
   sudo systemctl start postgresql
   ```

2. **Create Database**:
   ```bash
   # Login to PostgreSQL
   sudo -u postgres psql
   
   # Create database
   CREATE DATABASE coffee_shop_db;
   CREATE USER your_username WITH PASSWORD 'your_password';
   GRANT ALL PRIVILEGES ON DATABASE coffee_shop_db TO your_username;
   ```

3. **Configure Environment**:
   ```bash
   # Copy environment template
   cp env_example.txt .env
   
   # Edit .env file
   DB_HOST=localhost
   DB_PORT=5432
   DB_NAME=coffee_shop_db
   DB_USER=your_username
   DB_PASSWORD=your_password
   ```

## 📖 Usage Guide

### 1. Duplicate Detection

```python
from duplicate_checker import DuplicateChecker

# Load data
import pandas as pd
df = pd.read_csv('coffee_shop_data.csv')

# Initialize checker
checker = DuplicateChecker()

# Detect duplicates
duplicates = checker.detect_duplicates(df)

# Clean duplicates
cleaned_df = checker.clean_duplicates(df, duplicates)
```

## 📈 Next Steps

### Advanced Features

1. **Real-time Data Pipeline**:
   - Schedule data collection
   - Auto-detect duplicates
   - Re-train ML models

2. **Advanced ML**:
   - Random Forest
   - XGBoost
   - Neural Networks

3. **Deployment**:
   - Docker containers
   - Cloud deployment (AWS/GCP)
   - API endpoints

4. **Monitoring**:
   - Data quality metrics
   - Model performance tracking
   - Alert systems

## 🤝 Contributing

1. Fork the project
2. Create feature branch
3. Commit changes
4. Push to branch
5. Open Pull Request

---

**Happy Data Cleaning & Machine Learning! 🚀**