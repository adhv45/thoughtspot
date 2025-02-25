
# 📊 ThoughtSpot Dagster Project 

## 📌  Intsructions to run the project on MacOS
Step 1: Create virtual env with python3.12 and activate it
```
python3 -m virtualenv venv
source venv/bin/activate
```
Step 2: Install required dependencies
```
pip install -r requirements.txt
```
Step 3: Clone the repository and navigate to the repository
```
git clone https://github.com/adhv45/thoughtspot.git
cd thoughspot/thoughtspot-project
```
Step 4: Run dagster
```
dagster dev
```
Step 5: Gp to http://localhost:3000/jobs and run the job to materialize it.

---
This project processes **transactional sales data** (`transactions.csv`) and **customer data** (`customer.csv`) using Dagster.

## 📌 Data Model Overview  

### **1️⃣ Table: `sales_data_partitioned` (Partitioned by Hourly `TRANSACTION_DATE`)**  
This table contains individual sales transactions.  

#### **🔹 Schema:**
| Column Name       | Data Type  | Description |
|------------------|-----------|------------|
| `TRANSACTION_ID` | STRING     | Unique identifier for each transaction |
| `CUSTOMER_ID`    | STRING     | Foreign key linking to `customers` table |
| `PRODUCT_ID`     | STRING     | ID of the product purchased |
| `QUANTITY`       | INTEGER    | Number of units bought |
| `PRICE`         | FLOAT      | Price per unit |
| `TRANSACTION_DATE` | TIMESTAMP | Date and time of the transaction (Used for partitioning) |

#### **📌 Partitioning:**  
- This table is **partitioned hourly** for efficient querying.
- Each partition represents one hour of transactional data.

---

### **2️⃣ Table: `customers` (Non-Partitioned)**  
This table stores customer demographic information.  

#### **🔹 Schema:**
| Column Name | Data Type  | Description |
|-------------|-----------|------------|
| `CUSTOMER_ID` | STRING | Primary key (Unique identifier for customers) |
| `NAME`        | STRING | Full name of the customer |
| `AGE`         | INTEGER | Age of the customer |
| `COUNTRY`     | STRING | Country where the customer is located |

---

### **3️⃣ Table: `customer_aggregated_data` (Joined Output Table)**  
This is the final materialized table that joins `transactions` and `customers` on `CUSTOMER_ID` to enrich sales data with customer information.

#### **🔹 Schema:**
| Column Name       | Data Type  | Source Table |
|------------------|-----------|--------------|
| `CUSTOMER_ID`    | STRING     | transactions/customers |
| `NAME`          | STRING     | customers |
| `AGE`           | INTEGER    | customers |
| `COUNTRY`       | STRING     | customers |
| `PRODUCT_ID`     | STRING     | transactions |
| `QUANTITY`       | INTEGER    | transactions |
| `PRICE`         | FLOAT      | transactions |


---
#### **📌 Reasoning:**  
- Joins transactional data with **customer information** for **richer insights**.  
- Enables analysis of **who** bought **what** and **when**.  
- Used for **business intelligence (BI) and reporting dashboards**.
---
## 📊 Business Use Cases  
This dataset supports multiple **business and analytical** use cases:  

1️⃣ **Sales Performance Analysis**  
   - Identify **top-selling products** and **revenue trends**.  
   - Track **hourly** sales patterns to optimize marketing.  

2️⃣ **Customer Demographics Insights**  
   - Segment customers by **age, country, and purchase behavior**.  
   - Personalize recommendations based on **customer preferences**.  

3️⃣ **Trend Forecasting**  
   - Analyze **seasonal demand** and stock levels.  
   - Predict future sales based on **historical transaction data**.  

---

## 🛠️ Future Enhancements  

🔹 **Data Quality Checks**  
- Implement **validations** to handle **missing or inconsistent data**.  
- Ensure no duplicate transactions exist before processing.  

🔹 **Automation & One-Click Deployment**  
- Implement **CI/CD pipelines** for **automated deployments**.  
- Enable **one-click deployment** using **Docker & Kubernetes** for seamless environment setup.  
- Automate **data pipeline execution** using **Dagster schedules & sensors**.  
- Integrate with **Terraform or AWS CDK** for **infrastructure as code (IaC)**.  

🔹 **Incremental Data Processing**  
- Instead of processing the full dataset, implement **incremental updates** based on new transactions.  

🔹 **Scalability Improvements**  
- Optimize queries by **adding indexes** where necessary.  
- Consider migrating to a **cloud data warehouse** or building a **data lake** for larger datasets. 
