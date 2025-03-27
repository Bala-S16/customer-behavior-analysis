import mysql.connector as db
import pandas as pd

# Function to read database configuration from a file
def read_db_config(filename):
    config = {}
    with open(filename, 'r') as file:
        for line in file:
            key, value = line.strip().split("=")
            config[key] = value
    return config

# Load database configuration
db_config = read_db_config("db_config.txt")

# Connect to MySQL
db_connect = db.connect(
    host=db_config["host"],
    user=db_config["user"],
    password=db_config["password"],
    database=db_config["database"]
)

cursor = db_connect.cursor()

# Drop tables if they exist
DROP_TABLES = [
    "DROP TABLE IF EXISTS Engagement_Data;",
    "DROP TABLE IF EXISTS Customer_Reviews;",
    "DROP TABLE IF EXISTS Customer_Journey;",
    "DROP TABLE IF EXISTS Geography;",
    "DROP TABLE IF EXISTS Products;",
    "DROP TABLE IF EXISTS Customer;"
]

# Execute drop table queries
for drop_query in DROP_TABLES:
    cursor.execute(drop_query)

# Dictionary of table creation queries
TABLES = {
    "Customer": """
        CREATE TABLE IF NOT EXISTS Customer (
            CustomerID INT PRIMARY KEY,
            CustomerName VARCHAR(255),
            Email VARCHAR(255),
            Gender VARCHAR(10),
            Age INT,
            GeographyID INT
        );
    """,
    "Products": """
        CREATE TABLE IF NOT EXISTS Products (
            ProductID INT PRIMARY KEY,
            ProductName VARCHAR(255),
            Category VARCHAR(255),
            Price DECIMAL(10,2)
        );
    """,
    "Geography": """
        CREATE TABLE IF NOT EXISTS Geography (
            GeographyID INT PRIMARY KEY,
            Country VARCHAR(255),
            City VARCHAR(255)
        );
    """,
    "Customer_Journey": """
        CREATE TABLE IF NOT EXISTS Customer_Journey (
            JourneyID INT PRIMARY KEY,
            CustomerID INT,
            ProductID INT,
            VisitDate DATE,
            Stage VARCHAR(50),
            Action VARCHAR(50),
            Duration INT,
            FOREIGN KEY (CustomerID) REFERENCES Customer(CustomerID),
            FOREIGN KEY (ProductID) REFERENCES Products(ProductID)
        );
    """,
    "Customer_Reviews": """
        CREATE TABLE IF NOT EXISTS Customer_Reviews (
            ReviewID INT PRIMARY KEY,
            CustomerID INT,
            ProductID INT,
            ReviewDate DATE,
            Rating INT,
            ReviewText TEXT,
            FOREIGN KEY (CustomerID) REFERENCES Customer(CustomerID),
            FOREIGN KEY (ProductID) REFERENCES Products(ProductID)
        );
    """,
    "Engagement_Data": """
        CREATE TABLE IF NOT EXISTS Engagement_Data (
            EngagementID INT PRIMARY KEY,
            ContentID INT,
            ContentType VARCHAR(255),
            Likes INT,
            EngagementDate DATE,
            CampaignID INT,
            ProductID INT,
            ViewsClicksCombined VARCHAR(255),
            FOREIGN KEY (ProductID) REFERENCES Products(ProductID)
        );
    """
}

# Create tables if they don’t exist
for table_name, table_sql in TABLES.items():
    cursor.execute(table_sql)
db_connect.commit()

# Function to insert CSV data into MySQL
def insert_csv_to_mysql(csv_file, table_name, db_connect):
    df = pd.read_csv(csv_file)  # Read CSV file
    cursor = db_connect.cursor()

    # Prepare SQL insert statement
    cols = ", ".join(df.columns)
    placeholders = ", ".join(["%s"] * len(df.columns))
    sql = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"

    # Insert each row
    for row in df.itertuples(index=False, name=None):
        cursor.execute(sql, row)

    db_connect.commit()  # Save changes
    print(f"✅ Data inserted into {table_name}")

# List of CSV files and their corresponding table names
CSV_FILES = {
    "Customer.csv": "Customer",
    "Products.csv": "Products",
    "Geography.csv": "Geography",
    "Customer_Journey.csv": "Customer_Journey",
    "Customer_Reviews.csv": "Customer_Reviews",
    "Engagement_Data.csv": "Engagement_Data"
}

# Insert data from CSV files
for csv_file, table in CSV_FILES.items():
    try:
        insert_csv_to_mysql(csv_file, table, db_connect)
    except FileNotFoundError:
        print(f"File not found: {csv_file}")

# Close connection
cursor.close()
db_connect.close()
print("All data inserted successfully & connection closed.")
