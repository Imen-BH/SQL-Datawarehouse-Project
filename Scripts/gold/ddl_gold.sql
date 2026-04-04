/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view  combines data from the Silver layer 
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO
create view gold.dim_customers as 
select ROW_NUMBER() over (order by cst_id) as customer_key,-- surrogate key (-- we generate surrogate key to not be depedent on the source system )
cst_id as customer_id ,cst_key as customer_number,cst_firstname as first_name ,cst_lastname as last_name ,
lla.cntry as country ,cst_marital_status as marital_status,
case when ci.cst_gndr !='n/a' then ci.cst_gndr --CRM is the master of gender info 
     else coalesce(ca.gen,'n/a')
end as gender ,ca.bdate as birthdate,
cst_create_date as create_date 
from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca on ci.cst_key=ca.cid
left join silver.erp_loc_a101 lla  on ci.cst_key=lla.cid
GO
-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================
 IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO 
create view  gold.dim_products as
select ROW_NUMBER() over (order by pn.prd_start_dt,pn.prd_key) as product_key,-- surrogate key 
 prd_id as product_id, prd_key as product_number,prd_nm as product_name,cat_id as category_id, 
pc.cat as category,pc.subcat as subcategory ,pc.maintenance,prd_cost as cost,prd_line as product_line,
prd_start_dt  as start_date
from silver.crm_prd_info pn
left join silver.erp_px_cat_g1v2 pc
on pn.cat_id=pc.id
where prd_end_dt is null-- filter out all historical data 

 GO 
-- =============================================================================
-- Create fact table : gold.fact_sales
-- =============================================================================
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO
--- fct table 
create view gold.fact_sales as 


select sls_ord_num as order_number ,pr.product_key, cu.customer_key,sls_order_dt as order_date,sls_ship_dt as ship_date,
sls_due_dt as due_date ,sls_sales as sales_amount,
sls_quantity as quantity,sls_price as price 
from silver.crm_sales_details sd
left join gold.dim_products pr
on sd.sls_prd_key=pr.product_number
left join gold.dim_customers cu 
on sd.sls_cust_id=cu.customer_id
GO



