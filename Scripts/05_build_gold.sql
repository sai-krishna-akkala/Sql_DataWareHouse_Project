/******************************************************************************************
  PURPOSE:
  This script builds the **Gold Layer** of the data warehouse by creating essential views:
  
    1. `gold.dim_customers` - Customer dimension view
    2. `gold.dim_products`  - Product dimension view
    3. `gold.fact_sales`    - Sales fact view

  These views join and transform data from the Silver Layer to create clean, analytical models
  ready for use in BI tools such as Power BI or Tableau.

  WARNING:
  - Ensure all **Silver Layer** views and tables (under `silver` schema) exist and are up-to-date.
  - This script assumes no duplicates or missing keys in Silver Layer joins.
  - Historical products (`prd_end_dt is not null`) are excluded in `dim_products`.
  - Any downstream dashboard logic relies on these views—validate after deployment.

  RECOMMENDED:
  - Run this script in a staging/test environment before pushing to production.
  - Version control changes to maintain audit trail of schema evolution.
******************************************************************************************/

--building the gold layer
--customers view (dimension)
create view gold.dim_customers as
	select
	   row_number() over(order by cst_id) as customer_key,
	   ci.cst_id customer_id,
	   ci.cst_key customer_number,
	   ci.cst_firstname  first_name,
	   ci.cst_lastname   last_name,
	   ci.cst_marital_status  marital_status,
	   la.cntry country,
	   case when ci.cst_gndr!='n/a' then ci.cst_gndr
			else coalesce(ca.gen,'n/a')
		end as gender,
	   ca.bdate  birthdate,
	   ci.cst_create_date 
	from silver.crm_cust_info ci
	left join silver.erp_cust_az12 ca
	on ci.cst_key=ca.cid
	left join silver.erp_loc_a101 la
	on ci.cst_key=la.cid
--products view (dimension)
create or alter view gold.dim_products as
	select
		row_number() over(order by p.prd_start_dt,p.prd_key) as product_key,
		p.prd_id product_i,
		p.prd_key product_number,
		p.prd_nm  product_name,
		p.cat_id category_id,
		pc.cat category,
		pc.subcat  sub_category,
		pc.maintenance maintenance,
		p.prd_cost  cost,
		p.prd_line   product_line,
		p.prd_start_dt start_date
	from silver.crm_prd_info p
	left join silver.erp_px_cat_g1v2 pc
	on p.cat_id=pc.id
	where p.prd_end_dt is null --filtering out historical data
--sales data (fact)
create or alter view gold.fact_sales as
	select
	    s.sls_ord_num,
		p.product_key,
		c.customer_key,
		s.sls_order_dt,
		s.sls_ship_dt,
		s.sls_due_dt,
		s.sls_sales,
		s.sls_quantity,
		s.sls_price
	from silver.crm_sales_details s
	left join gold.dim_products p
	on s.sls_prd_key=p.product_number
	left join gold.dim_customers c
	on s.sls_cust_id=c.customer_key

