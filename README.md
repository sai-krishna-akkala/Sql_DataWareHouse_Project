
# 🏗️ CRM + ERP SQL Data Warehouse Project

A comprehensive **Data Warehouse** built using T-SQL to integrate and transform CRM and ERP datasets into clean, analysis-ready reporting views for business intelligence.

---

### 📌 Data Warehouse Architecture
![Architecture Diagram](https://drive.google.com/uc?export=view&id=1QQNkvsgi0UXhxKg1r_3Sp33t5enS3O5t)

**Layers:**
- **Source**: Raw CSV files from CRM & ERP systems  
- **Bronze**: Raw ingested tables  
- **Silver**: Cleaned and conformed data  
- **Gold**: Final dimensional views for reporting

---
### Data Flow 
![Data Flow Diagram](https://drive.google.com/uc?export=view&id=1PObTRx9JagAKqo7aUtnnYgv3SVwjpaqz)

---
### ✅ Key Features

- Ingested 6 structured CRM/ERP datasets
- Cleaned and normalized raw data in Silver layer
- Built 3 dimensional views in Gold:
  - `dim_customers`
  - `dim_products`
  - `fact_sales`
- Ready for integration with BI tools (Power BI/Tableau)

---

### 🔧 ETL Process

#### 🥉 Bronze Layer
- Loaded CSVs into:
  - `crm_cust_info`, `crm_prd_info`, `crm_sales_details`
  - `erp_cust_az12`, `erp_loc_a101`, `erp_px_cat_g1v2`

#### 🥈 Silver Layer
- Standardized country names, product categories
- Cleaned duplicates and nulls
- Derived product categories, cost bands

#### 🥇 Gold Layer
- `dim_customers`: customer demographics + segmentation  
- `dim_products`: product details + categories  
- `fact_sales`: sales facts joined with dimensions

---

### 📊 Business Outcomes

- Segment customers by region, gender, and lifecycle value
- Analyze product performance by category and sales
- Track YoY trends and monthly revenue movements

---

### 🧰 Tools & Techniques

- SQL Server (SSMS)
- T-SQL: Joins, CTEs, Window Functions, Views
- Data Modeling: Star Schema (Facts + Dimensions)
- Analytics-ready schema for BI visualization

---

### 📎 Sample Output: `fact_sales`

| sls\_ord\_num | product\_key | customer\_key | sls\_order\_dt | sls\_sales | sls\_quantity | sls\_price |
| ------------- | ------------ | ------------- | -------------- | ---------- | ------------- | ---------- |
| SO43697       | 20           | **11005**     | 2010-12-29     | 3578       | 1             | 3578       |
| SO43698       | 9            | **11003**     | 2010-12-29     | 3400       | 1             | 3400       |
| SO43699       | 9            | **11003**     | 2010-12-29     | 3400       | 1             | 3400       |
| SO43700       | 47           | 14501         | 2010-12-29     | 699        | 1             | 699        |


---

### 👤 Author

Hi, I'm **Akkala Sai Krishna** — a data engineering enthusiast passionate about building scalable data solutions with SQL and modern warehouse design.

🔗 [LinkedIn](https://www.linkedin.com/in/sai-krishna-akkala07/)

---

### 🏷️ Tags  
`#SQL` `#DataWarehouse` `#ETL` `#CRM` `#ERP` `#FactDimensionModel` `#TSQL` `#BI`
