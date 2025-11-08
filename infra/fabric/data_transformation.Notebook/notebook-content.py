# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "dc2f6ddc-834f-4554-bde5-59dcc312d0be",
# META       "default_lakehouse_name": "ZavaOps_LH",
# META       "default_lakehouse_workspace_id": "bbae4f79-0b3f-4e6f-8849-531020cd6602",
# META       "known_lakehouses": [
# META         {
# META           "id": "dc2f6ddc-834f-4554-bde5-59dcc312d0be"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

   %%sql
   CREATE MATERIALIZED LAKE VIEW IF NOT EXISTS silver.customers_curated
   (
       CONSTRAINT valid_email CHECK (EmailNormalized LIKE '%@%') ON MISMATCH DROP
   )
   COMMENT 'Clean customer dimension with normalized names and emails.'
   AS
   SELECT
       CustomerId,
       INITCAP(FirstName) AS FirstName,
       INITCAP(LastName) AS LastName,
       LOWER(Email) AS EmailNormalized,
       Phone,
       Address,
       City,
       State,
       Country,
       PreferredLanguage,
       CustomerSegment,
       RegistrationDate,
       CAST(RegistrationDate AS DATE) AS RegistrationDateKey,
       CONCAT_WS(' ', INITCAP(FirstName), INITCAP(LastName)) AS FullName
   FROM bronze.customers;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC    CREATE MATERIALIZED LAKE VIEW IF NOT EXISTS silver.online_sales_enriched
# MAGIC    (
# MAGIC        CONSTRAINT online_line_total CHECK (LineTotalVarianceCents BETWEEN -5 AND 5) ON MISMATCH DROP
# MAGIC    )
# MAGIC    COMMENT 'Online sales fact grain with customer, product, and fulfillment context.'
# MAGIC    AS
# MAGIC    SELECT
# MAGIC        o.OnlineOrderId AS TransactionId,
# MAGIC        o.CustomerId,
# MAGIC        c.FirstName AS CustomerFirstName,
# MAGIC        c.LastName AS CustomerLastName,
# MAGIC        c.CustomerSegment,
# MAGIC        c.Country AS CustomerCountry,
# MAGIC        CAST(o.OrderDate AS DATE) AS TransactionDate,
# MAGIC        o.OrderStatus,
# MAGIC        o.PaymentMethod,
# MAGIC        o.PaymentStatus,
# MAGIC        o.ShipDate,
# MAGIC        o.DeliveryDate,
# MAGIC        p.ProductId,
# MAGIC        p.ProductName,
# MAGIC        p.ProductType,
# MAGIC        p.CategoryId,
# MAGIC        cat.CategoryName,
# MAGIC        cat.CategoryType,
# MAGIC     l.Quantity,
# MAGIC     l.UnitPrice,
# MAGIC     l.LineTotal,
# MAGIC     ROUND(l.Quantity * l.UnitPrice, 2) AS CalculatedLineTotal,
# MAGIC     ROUND(l.LineTotal - (l.Quantity * l.UnitPrice), 2) AS LineTotalVariance,
# MAGIC          ROUND((l.LineTotal - (l.Quantity * l.UnitPrice)) * 100, 0) AS LineTotalVarianceCents,
# MAGIC        p.CostPrice,
# MAGIC        p.MSRP,
# MAGIC        o.SubTotal,
# MAGIC        o.TaxAmount,
# MAGIC        o.ShippingAmount,
# MAGIC        o.DiscountAmount,
# MAGIC        o.TotalAmount,
# MAGIC        'Online' AS SalesChannel
# MAGIC    FROM bronze.online_orders o
# MAGIC    JOIN bronze.online_order_line_items l
# MAGIC        ON o.OnlineOrderId = l.OnlineOrderId
# MAGIC    LEFT JOIN bronze.customers c
# MAGIC        ON o.CustomerId = c.CustomerId
# MAGIC    LEFT JOIN bronze.products p
# MAGIC        ON l.ProductId = p.ProductId
# MAGIC    LEFT JOIN bronze.product_categories cat
# MAGIC        ON p.CategoryId = cat.CategoryId;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC    CREATE MATERIALIZED LAKE VIEW IF NOT EXISTS silver.store_sales_enriched
# MAGIC    (
# MAGIC        CONSTRAINT store_line_total CHECK (LineTotalVarianceCents BETWEEN -5 AND 5) ON MISMATCH DROP
# MAGIC    )
# MAGIC    COMMENT 'In-store sales fact grain with store, associate, and product enrichment.'
# MAGIC    AS
# MAGIC    SELECT
# MAGIC        s.InStoreSaleId AS TransactionId,
# MAGIC        s.StoreId,
# MAGIC        st.StoreName,
# MAGIC        st.StoreCode,
# MAGIC        st.StoreType,
# MAGIC        s.CustomerId,
# MAGIC        c.FirstName AS CustomerFirstName,
# MAGIC        c.LastName AS CustomerLastName,
# MAGIC        CAST(s.SaleDate AS DATE) AS TransactionDate,
# MAGIC        s.PaymentMethod,
# MAGIC        s.SalesEmployeeId,
# MAGIC        emp.FirstName AS SalesFirstName,
# MAGIC        emp.LastName AS SalesLastName,
# MAGIC        emp.JobTitle AS SalesJobTitle,
# MAGIC        dept.DepartmentName AS SalesDepartment,
# MAGIC        p.ProductId,
# MAGIC        p.ProductName,
# MAGIC        p.ProductType,
# MAGIC        p.CategoryId,
# MAGIC        cat.CategoryName,
# MAGIC      cat.CategoryType,
# MAGIC      li.Quantity,
# MAGIC      li.UnitPrice,
# MAGIC      li.LineTotal,
# MAGIC      ROUND(li.Quantity * li.UnitPrice, 2) AS CalculatedLineTotal,
# MAGIC      ROUND(li.LineTotal - (li.Quantity * li.UnitPrice), 2) AS LineTotalVariance,
# MAGIC      ROUND((li.LineTotal - (li.Quantity * li.UnitPrice)) * 100, 0) AS LineTotalVarianceCents,
# MAGIC        p.CostPrice,
# MAGIC        p.MSRP,
# MAGIC        s.SubTotal,
# MAGIC        s.TaxAmount,
# MAGIC        CAST(0 AS DECIMAL(18, 2)) AS ShippingAmount,
# MAGIC        s.DiscountAmount,
# MAGIC        s.TotalAmount,
# MAGIC        'InStore' AS SalesChannel
# MAGIC    FROM bronze.in_store_sales s
# MAGIC    JOIN bronze.in_store_sale_line_items li
# MAGIC        ON s.InStoreSaleId = li.InStoreSaleId
# MAGIC    LEFT JOIN bronze.customers c
# MAGIC        ON s.CustomerId = c.CustomerId
# MAGIC    LEFT JOIN bronze.retail_stores st
# MAGIC        ON s.StoreId = st.StoreId
# MAGIC    LEFT JOIN bronze.employees emp
# MAGIC        ON s.SalesEmployeeId = emp.EmployeeId
# MAGIC    LEFT JOIN bronze.departments dept
# MAGIC        ON emp.DepartmentId = dept.DepartmentId
# MAGIC    LEFT JOIN bronze.products p
# MAGIC        ON li.ProductId = p.ProductId
# MAGIC    LEFT JOIN bronze.product_categories cat
# MAGIC        ON p.CategoryId = cat.CategoryId;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC    CREATE MATERIALIZED LAKE VIEW IF NOT EXISTS gold.daily_channel_performance
# MAGIC    COMMENT 'Daily revenue, transaction counts, and gross margin by Zava sales channel.'
# MAGIC    AS
# MAGIC    WITH unified_sales AS (
# MAGIC        SELECT
# MAGIC            TransactionId,
# MAGIC            CustomerId,
# MAGIC            TransactionDate,
# MAGIC            SalesChannel,
# MAGIC            ProductId,
# MAGIC            ProductName,
# MAGIC            CategoryId,
# MAGIC            CategoryName,
# MAGIC            CategoryType,
# MAGIC            Quantity,
# MAGIC            LineTotal,
# MAGIC            CostPrice,
# MAGIC            SubTotal,
# MAGIC            TaxAmount,
# MAGIC            ShippingAmount,
# MAGIC            DiscountAmount,
# MAGIC            TotalAmount
# MAGIC        FROM silver.online_sales_enriched
# MAGIC        UNION ALL
# MAGIC        SELECT
# MAGIC            TransactionId,
# MAGIC            CustomerId,
# MAGIC            TransactionDate,
# MAGIC            SalesChannel,
# MAGIC            ProductId,
# MAGIC            ProductName,
# MAGIC            CategoryId,
# MAGIC            CategoryName,
# MAGIC            CategoryType,
# MAGIC            Quantity,
# MAGIC            LineTotal,
# MAGIC            CostPrice,
# MAGIC            SubTotal,
# MAGIC            TaxAmount,
# MAGIC            ShippingAmount,
# MAGIC            DiscountAmount,
# MAGIC            TotalAmount
# MAGIC        FROM silver.store_sales_enriched
# MAGIC    )
# MAGIC    SELECT
# MAGIC        TransactionDate AS SalesDate,
# MAGIC        SalesChannel,
# MAGIC        SUM(Quantity) AS UnitsSold,
# MAGIC        SUM(LineTotal) AS GrossRevenue,
# MAGIC        SUM(Quantity * CostPrice) AS EstimatedCostOfGoods,
# MAGIC        SUM(LineTotal) - SUM(Quantity * CostPrice) AS GrossMargin,
# MAGIC        CASE WHEN SUM(LineTotal) = 0 THEN NULL ELSE ROUND((SUM(LineTotal) - SUM(Quantity * CostPrice)) / SUM(LineTotal), 4) END AS GrossMarginRatio,
# MAGIC        SUM(TotalAmount) AS NetRevenue,
# MAGIC        SUM(TaxAmount) AS TaxCollected,
# MAGIC        SUM(ShippingAmount) AS ShippingCollected,
# MAGIC        SUM(DiscountAmount) AS DiscountGiven,
# MAGIC        COUNT(DISTINCT TransactionId) AS Transactions,
# MAGIC        SUM(CASE WHEN CustomerId IS NOT NULL THEN 1 ELSE 0 END) AS KnownCustomerTransactions
# MAGIC    FROM unified_sales
# MAGIC    GROUP BY TransactionDate, SalesChannel;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC    CREATE MATERIALIZED LAKE VIEW IF NOT EXISTS gold.top_sku_performance_30d
# MAGIC    COMMENT 'Rolling 30-day revenue and margin leaders across both sales channels.'
# MAGIC    AS
# MAGIC    WITH combined AS (
# MAGIC        SELECT TransactionDate, ProductId, ProductName, CategoryName, CategoryType, SalesChannel, Quantity, LineTotal, CostPrice
# MAGIC        FROM silver.online_sales_enriched
# MAGIC        UNION ALL
# MAGIC        SELECT TransactionDate, ProductId, ProductName, CategoryName, CategoryType, SalesChannel, Quantity, LineTotal, CostPrice
# MAGIC        FROM silver.store_sales_enriched
# MAGIC    )
# MAGIC    SELECT
# MAGIC        current_date() AS SnapshotDate,
# MAGIC        ProductId,
# MAGIC        ProductName,
# MAGIC        CategoryName,
# MAGIC        CategoryType,
# MAGIC        SUM(Quantity) AS UnitsSoldLast30Days,
# MAGIC        SUM(LineTotal) AS GrossRevenueLast30Days,
# MAGIC        SUM(LineTotal) - SUM(Quantity * CostPrice) AS GrossMarginLast30Days,
# MAGIC        COUNT(DISTINCT SalesChannel) AS ActiveChannels
# MAGIC    FROM combined
# MAGIC    WHERE TransactionDate >= date_sub(current_date(), 30)
# MAGIC    GROUP BY ProductId, ProductName, CategoryName, CategoryType
# MAGIC    ORDER BY GrossRevenueLast30Days DESC
# MAGIC    LIMIT 100;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
