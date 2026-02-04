from app import app, db
from sqlalchemy import text
import os

print(f"Current URI: {app.config.get('SQLALCHEMY_DATABASE_URI')}")

try:
    with app.app_context():
        db.create_all()
        print("Tables created successfully.")
        
        # Test query
        result = db.session.execute(text("SELECT 1")).fetchone()
        print(f"Connection Test: {result}")
        
        # Check if tables exist
        inspector = db.inspect(db.engine)
        tables = inspector.get_table_names()
        print(f"Tables found: {tables}")
        
except Exception as e:
    print(f"Error: {e}")
