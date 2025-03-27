import mysql.connector as db
import pandas as pd

# Read database config from file
def read_db_config(filename):
    config = {}
    with open(filename, 'r') as file:
        for line in file:
            key, value = line.strip().split("=")
            config[key] = value
    return config

# Load DB config
db_config = read_db_config("db_config.txt")

# Connect to MySQL
conn = db.connect(
    host=db_config["host"],
    user=db_config["user"],
    password=db_config["password"],
    database=db_config["database"]
)
cursor = conn.cursor()

# Fetch Engagement Data
cursor.execute("SELECT ProductID, EngagementDate, CampaignID, ContentType, Likes, ViewsClicksCombined FROM engagement_Data")
data_engagement = cursor.fetchall()
df_engagement = pd.DataFrame(data_engagement, columns=["ProductID", "EngagementDate", "CampaignID", "ContentType", "Likes", "ViewsClicksCombined"])

# Fetch Product Data
cursor.execute("SELECT ProductID, Category,ProductName FROM products")
data_products = cursor.fetchall()
df_products = pd.DataFrame(data_products, columns=["ProductID", "Category", "ProductName"])

cursor.execute("SELECT Stage, Action FROM customer_journey")
data_journey = cursor.fetchall()
df_journey = pd.DataFrame(data_journey, columns=["Stage", "Action"])

# Fetch Customer Reviews Data
cursor.execute("SELECT ProductID, Rating FROM customer_reviews")
data_reviews = cursor.fetchall()
df_reviews = pd.DataFrame(data_reviews, columns=["ProductID", "Rating"])

# Fetch Purchase Data (Customers who completed a purchase)
cursor.execute("SELECT CustomerID,ProductID FROM customer_journey WHERE Action = 'purchase'")
data_purchases = cursor.fetchall()
df_purchases = pd.DataFrame(data_purchases, columns=["CustomerID","ProductID"])

# Fetch Customer Data (Already Linked to Purchases)
cursor.execute("SELECT CustomerID, Gender, Age, GeographyID FROM customer")
data_customers = cursor.fetchall()
df_customers = pd.DataFrame(data_customers, columns=["CustomerID", "Gender", "Age", "GeographyID"])

# Fetch Geography Data
cursor.execute("SELECT GeographyID, Country, City FROM geography")
data_geography = cursor.fetchall()
df_geography = pd.DataFrame(data_geography, columns=["GeographyID", "Country", "City"])

# Close MySQL Connection
cursor.close()
conn.close()

# Convert EngagementDate to Datetime
df_engagement["EngagementDate"] = pd.to_datetime(df_engagement["EngagementDate"], errors='coerce')

# Extract Only the Date (Remove Time)
df_engagement["EngagementDate"] = df_engagement["EngagementDate"].dt.date

# Handle Missing Values in ViewsClicksCombined
df_engagement["ViewsClicksCombined"].fillna("0-0", inplace=True)

# Fix ViewsClicksCombined (Split into Views & Clicks)
df_engagement[['Views', 'Clicks']] = df_engagement['ViewsClicksCombined'].str.split('-', expand=True).astype(int)

# Calculate Total Engagement (Likes + Views + Clicks)
df_engagement["TotalEngagement"] = df_engagement["Likes"] + df_engagement["Views"] + df_engagement["Clicks"]

# Analyze Engagement by Content Type
df_content = df_engagement.groupby("ContentType")["TotalEngagement"].sum().reset_index()
df_content = df_content.sort_values(by="TotalEngagement", ascending=False)
top_content = df_content.iloc[0]
print(f"🔹 Insight: 🎥 **{top_content['ContentType']}** receives the highest engagement with **{top_content['TotalEngagement']}** interactions.")

# Merge Engagement Data with Product Category
df_merged = df_engagement.merge(df_products, on="ProductID", how="left")

# Analyze Engagement by Product Category
df_category = df_merged.groupby("Category")["TotalEngagement"].sum().reset_index()
df_category = df_category.sort_values(by="TotalEngagement", ascending=False)
top_category = df_category.iloc[0]
print(f"🔹 Insight: ⚡ **{top_category['Category']}** products have the highest engagement with **{top_category['TotalEngagement']}** interactions.")

# Analyze Engagement by Campaign
df_campaign = df_engagement.groupby("CampaignID")["TotalEngagement"].sum().reset_index()
df_campaign = df_campaign.sort_values(by="TotalEngagement", ascending=False)
top_campaign = df_campaign.iloc[0]
print(f"🔹 Insight: 📢 **Campaign {top_campaign['CampaignID']}** was the most successful, generating **{top_campaign['TotalEngagement']}** interactions.")

# Analyze Engagement by Date
df_daily = df_engagement.groupby("EngagementDate")["TotalEngagement"].sum().reset_index()
df_daily = df_daily.sort_values(by="TotalEngagement", ascending=False)
top_date = df_daily.iloc[0]
print(f"🔹 Insight: 📅 The highest engagement happened on **{top_date['EngagementDate']}** with **{top_date['TotalEngagement']}** interactions.")


# Standardize Data (Fix Stage Names & Actions)
df_journey["Stage"] = df_journey["Stage"].str.strip().str.lower()
df_journey["Action"] = df_journey["Action"].str.strip().str.lower()

# Debug - Check Unique Stages
print("DEBUG: Unique Stages in Data:")
print(df_journey["Stage"].unique())

# Count Total Users at Each Stage
df_stage_counts = df_journey.groupby("Stage").size().reset_index(name="TotalUsers")

# Count Drop-Offs (Now "dropoff" should match correctly)
df_dropoffs = df_journey[df_journey["Action"] == "drop-off"].groupby("Stage").size().reset_index(name="DropOffs")

# Debug - Check Drop-Off Data
print("DEBUG: Drop-Off Data Before Merging (After Fix):")
print(df_dropoffs)

# Merge Drop-Offs with Total Users
df_final = df_stage_counts.merge(df_dropoffs, on="Stage", how="left")

# Fix: Use fillna() without inplace=True
df_final["DropOffs"] = df_final["DropOffs"].fillna(0).astype(int)

# Calculate Drop-Off Percentage
df_final["DropOffPercentage"] = df_final.apply(lambda row: 
    (row["DropOffs"] / row["TotalUsers"]) * 100 if row["TotalUsers"] > 0 else 0, axis=1)

# Sort by Drop-Off Percentage (Descending)
df_final = df_final.sort_values(by="DropOffPercentage", ascending=False)

# Print Drop-Off Percentages in Required Format
print("\n🔹 Drop-Off Percentages by Stage:")
for _, row in df_final.iterrows():
    print(f"{row['Stage'].upper()} - {row['DropOffPercentage']:.2f}% ({int(row['DropOffs'])}/{int(row['TotalUsers'])})")

# Calculate Average Rating Per Product
df_avg_rating = df_reviews.groupby("ProductID")["Rating"].mean().reset_index()

# Count Purchases Per Product
df_purchase_count = df_purchases.groupby("ProductID").size().reset_index(name="TotalPurchases")

# Merge Reviews and Product Names (INNER JOIN)
df_merged = df_avg_rating.merge(df_products, on="ProductID", how="inner")

# Merge Purchases with Product Ratings (INNER JOIN)
df_final = df_merged.merge(df_purchase_count, on="ProductID", how="inner")

# Sort Products by Rating
df_final = df_final.sort_values(by="Rating", ascending=False)

# Print Product Name, Average Rating, and Total Purchases
print("\n🔹 Average Rating per Product:")
for _, row in df_final.iterrows():
    print(f"🛍 {row['ProductName']} - ⭐ {row['Rating']:.2f} Avg Rating, 🛒 {row['TotalPurchases']} Purchases")

# Insights on Best & Worst Rated Products
best_product = df_final.iloc[0]
worst_product = df_final.iloc[-1]

print(f"\n🔹 Insight: 🏆 **{best_product['ProductName']}** is the highest-rated product with ⭐ {best_product['Rating']:.2f}.")
print(f"🔹 Insight: ⚠️ **{worst_product['ProductName']}** has the lowest rating at ⭐ {worst_product['Rating']:.2f}, indicating possible quality issues or customer dissatisfaction.")

# Analyze Correlation Between Ratings and Purchases
correlation = df_final["Rating"].corr(df_final["TotalPurchases"])

# Print Correlation Insight
if correlation > 0.5:
    print(f"\n🔹 Insight: ⭐ Higher-rated products are strongly correlated with more purchases (Correlation: {correlation:.2f}).")
elif correlation > 0.2:
    print(f"\n🔹 Insight: 📈 There is a moderate relationship between high ratings and purchases (Correlation: {correlation:.2f}).")
else:
    print(f"\n🔹 Insight: ❌ Customer ratings have little impact on purchase behavior (Correlation: {correlation:.2f}).")

# Count Purchases Per Product
df_product_sales = df_purchases.groupby("ProductID").size().reset_index(name="TotalPurchases")

# Merge Product Data (INNER JOIN)
df_product_sales = df_product_sales.merge(df_products, on="ProductID", how="inner")

# Find Best-Selling Product
df_product_sales = df_product_sales.sort_values(by="TotalPurchases", ascending=False)
top_product = df_product_sales.iloc[0]
print(f"\n🔹 Insight: 🛍 **{top_product['ProductName']}** is the best-selling product with **{top_product['TotalPurchases']}** purchases.")

# Merge Purchases with Customer Data (INNER JOIN)
df_purchases_customers = df_purchases.merge(df_customers, on="CustomerID", how="inner")

# Merge with Geography Data (INNER JOIN)
df_purchases_customers = df_purchases_customers.merge(df_geography, on="GeographyID", how="inner")

# Count Purchases Per Location
df_location_sales = df_purchases_customers.groupby(["Country", "City"]).size().reset_index(name="TotalPurchases")

# Find Best-Performing Location
df_location_sales = df_location_sales.sort_values(by="TotalPurchases", ascending=False)
top_location = df_location_sales.iloc[0]
print(f"🔹 Insight: 📍 **{top_location['City']}, {top_location['Country']}** has the highest sales with **{top_location['TotalPurchases']}** purchases.")

# Count Purchases by Customer Segment (Gender & Age)
df_segment_sales = df_purchases_customers.groupby(["Gender"]).size().reset_index(name="TotalPurchases")
df_age_sales = df_purchases_customers.groupby(["Age"]).size().reset_index(name="TotalPurchases")

# Find Best Customer Segment
top_gender_segment = df_segment_sales.sort_values(by="TotalPurchases", ascending=False).iloc[0]
top_age_segment = df_age_sales.sort_values(by="TotalPurchases", ascending=False).iloc[0]

print(f"🔹 Insight: 👥 **{top_gender_segment['Gender']} customers** are the top buyers with **{top_gender_segment['TotalPurchases']}** purchases.")
print(f"🔹 Insight: 🎯 The **age group {top_age_segment['Age']}** has the highest purchases with **{top_age_segment['TotalPurchases']}** purchases.")

# print(f"DEBUG: Total Purchases Found: {df_purchases.shape[0]}")
# print("DEBUG: Unique Products in Purchases:", df_purchases["ProductID"].nunique())
# print("DEBUG: Sample Purchase Data Before Merge:")
# print(df_purchases.head())

# Merge Purchases with Customers to Get Region Info
df_purchases_customers = df_purchases.merge(df_customers, on="CustomerID", how="inner")

# Merge with Geography Data to Get Country & City
df_purchases_customers = df_purchases_customers.merge(df_geography, on="GeographyID", how="inner")

# Merge Purchases with Product Names
df_purchases_customers = df_purchases_customers.merge(df_products, on="ProductID", how="inner")

# Count Purchases Per Product Per Country
df_region_sales = df_purchases_customers.groupby(["Country", "ProductName"]).size().reset_index(name="TotalPurchases")

# Find Best-Selling Product in Each Country
df_best_products = df_region_sales.loc[df_region_sales.groupby("Country")["TotalPurchases"].idxmax()]

# Print Insights
print("\n🔹 Best-Performing Products Per Region:")
for _, row in df_best_products.iterrows():
    print(f"📍 In **{row['Country']}**, the best-selling product is 🛍 **{row['ProductName']}** with **{row['TotalPurchases']}** purchases.")