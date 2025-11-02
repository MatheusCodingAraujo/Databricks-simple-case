# Databricks notebook source
bronze_path   = 'abfss://uc-ext-azure@externalazure28.dfs.core.windows.net/bikestore/bronze/'
silver_path   = 'abfss://uc-ext-azure@externalazure28.dfs.core.windows.net/bikestore/silver/'
gold_path     = 'abfss://uc-ext-azure@externalazure28.dfs.core.windows.net/bikestore/gold/'
resource_path = 'abfss://uc-ext-azure@externalazure28.dfs.core.windows.net/bikestore/resource/origem/'
resource_path_volume = '/Volumes/bikestore/logistica/bikestore_resource/origem'

# COMMAND ----------

bronze_map = {
    "tmp_bronze_brands":      f"{bronze_path}/brands/",
    "tmp_bronze_categories":  f"{bronze_path}/categories/",
    #"tmp_bronze_customers":   f"{bronze_path}/customers/",
    #"tmp_bronze_order_items": f"{bronze_path}/order_items/",
    #"tmp_bronze_orders":      f"{bronze_path}/orders/",
    "tmp_bronze_products":    f"{bronze_path}/products/",
    #"tmp_bronze_staffs":      f"{bronze_path}/staffs/",
    "tmp_bronze_stocks":      f"{bronze_path}/stocks/",
    #"tmp_bronze_stores":      f"{bronze_path}/stores/",
}
for view_name, path in bronze_map.items():
    (spark.read.format('delta')
        .load(path)
        .createOrReplaceTempView(view_name))
 


# COMMAND ----------

df_product_silver = spark.sql("""
with
stock as ( 
select 
  product_id
  ,sum(quantity) as total_stock
from tmp_bronze_stocks
--where product_id = 1
group by product_id
)


select
   p.product_id
  ,p.product_name
  --,p.brand_id
  --,b.brand_id as brand_id_brand
  ,b.brand_name
  --,p.category_id
  --,c.category_id as category_id_categoriat
  ,c.category_name
  ,p.model_year
  ,p.list_price
  ,s.total_stock
from        tmp_bronze_products as p
left join tmp_bronze_categories as c on p.category_id = c.category_id
left join     tmp_bronze_brands as b on p.brand_id = b.brand_id
left join                 stock as s on p.product_id = s.product_id                             
                              
                              
                              """)

# salvar em Delta na silver 
df_product_silver.write\
    .mode('overwrite')\
    .format('delta')\
    .option('mergeSchema','true')\
    .save(f'{silver_path}/product')
