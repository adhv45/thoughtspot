import pandas as pd
import random
from faker import Faker

fake = Faker()

# Generate Transaction Data
transactions = []
for _ in range(234567):  # 1000 transactions
    transactions.append({
        "TRANSACTION_ID": fake.uuid4(),
        "CUSTOMER_ID": random.randint(1000, 2000),  
        "PRODUCT_ID": random.randint(1, 100),  
        "QUANTITY": random.randint(1, 10),  
        "PRICE": round(random.uniform(5.0, 5000.0), 2),  
        "TRANSACTION_DATE": fake.date_time_between(start_date="-30d", end_date="now"),
    })

df_transactions = pd.DataFrame(transactions)
print(df_transactions.head())
df_transactions.to_csv('data/transactions.csv', sep=',', index=False, header=True)


customers = []
for customer_id in range(1000, 2001):  
    customers.append({
        "CUSTOMER_ID": customer_id,  # Common Key
        "NAME": fake.name(),
        "AGE": random.randint(18, 65),
        "COUNTRY": fake.country(),
    })

df_customers = pd.DataFrame(customers)
print(df_customers.head())

df_customers.to_csv('data/customer.csv', sep=',', index=False, header=True)
