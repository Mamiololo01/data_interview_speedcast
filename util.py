# import libraries
from psycopg2.extras import execute_values
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from dotenv import dotenv_values
dotenv_values()

# Get credentials from environment variable file
config = dotenv_values('.env')

def get_database_conn():
    # Get database credentials from environment variable
    config = dict(dotenv_values('.env'))
    db_user_name = config.get('db_username')
    db_password = config.get('db_password')
    db_name = config.get('db_name')
    port = config.get('port')
    host = config.get('host')
    # Create and return a postgresql database connection object
    return create_engine(f'postgresql+psycopg2://{db_user_name}:{db_password}@{host}:{port}/{db_name}')

connection = get_database_conn()

def create_tables(engine):
    """Create tables if they don't exist."""
    with engine.connect() as connection:
        try:
            connection.execute(text(create_table_city))
            connection.execute(text(create_date_table))
            connection.execute(text(create_table_cloud_cover))
            connection.commit()
        except SQLAlchemyError as e:
            print(f"An error occurred while creating tables: {e}")
            connection.rollback()

def insert_into_dim_city(engine, cities):
    """Insert cities into dim_city table."""
    with engine.connect() as connection:
        for city in cities:
            try:
                connection.execute(text("INSERT INTO dim_city (city_name) VALUES (:city) ON CONFLICT DO NOTHING"), {"city": city})
                connection.commit()
            except SQLAlchemyError as e:
                print(f"An error occurred while inserting city {city}: {e}")
                connection.rollback()

def insert_into_dim_date(engine, start_date, end_date):
    """Insert dates into dim_date table."""
    with engine.connect() as connection:
        for date in pd.date_range(start=start_date, end=end_date):
            try:
                connection.execute(text("""
                    INSERT INTO dim_date (date, year, month, day) 
                    VALUES (:date, :year, :month, :day) 
                    ON CONFLICT DO NOTHING
                """), {
                    "date": date.date(),
                    "year": date.year,
                    "month": date.month,
                    "day": date.day
                })
                connection.commit()
            except SQLAlchemyError as e:
                print(f"An error occurred while inserting date {date}: {e}")
                connection.rollback()

def get_city_id(engine, city):
    """Get city_id for a given city name."""
    with engine.connect() as connection:
        result = connection.execute(text("SELECT city_id FROM dim_city WHERE city_name = :city"), {"city": city})
        return result.scalar()

def get_date_id(engine, date):
    """Get date_id for a given date."""
    with engine.connect() as connection:
        result = connection.execute(text("SELECT date_id FROM dim_date WHERE date = :date"), {"date": date})
        return result.scalar()

def insert_into_fact_cloud_cover(engine, data):
    """Insert data into fact_cloud_cover table."""
    with engine.connect() as connection:
        for _, row in data.iterrows():
            try:
                city_id = get_city_id(engine, row['city'])
                date = pd.to_datetime(row['time']).date()
                date_id = get_date_id(engine, date)
                time = pd.to_datetime(row['time']).time()
                
                connection.execute(text("""
                INSERT INTO fact_cloud_cover (city_id, date_id, time, cloud_cover, cloud_cover_low, cloud_cover_mid, cloud_cover_high)
                VALUES (:city_id, :date_id, :time, :cloud_cover, :cloud_cover_low, :cloud_cover_mid, :cloud_cover_high)
                """), {
                    "city_id": city_id,
                    "date_id": date_id,
                    "time": time,
                    "cloud_cover": row['cloud_cover'],
                    "cloud_cover_low": row['cloud_cover_low'],
                    "cloud_cover_mid": row['cloud_cover_mid'],
                    "cloud_cover_high": row['cloud_cover_high']
                })
                connection.commit()
            except SQLAlchemyError as e:
                print(f"An error occurred while inserting fact data: {e}")
                connection.rollback()

def main():
    engine = get_database_conn()
    
    cities = ["London", "Amsterdam", "Lisbon"]
    start_date = "2012-01-01"
    end_date = "2022-12-31"
    
    try:
        create_tables(engine)
        insert_into_dim_city(engine, cities)
        insert_into_dim_date(engine, start_date, end_date)
        insert_into_fact_cloud_cover(engine, combined_data)
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    main()