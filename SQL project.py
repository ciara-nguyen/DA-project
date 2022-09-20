# %% [markdown]
# In this project, we use Northwind database, which was orinally created by Microsofts and contains the sales data for a fictitious company called “Northwind Traders,” which imports and exports specialty foods from around the world. As a team of Data Analyst team , we use SQL to answer several business questions: 
#     Task 1: Track the sale situation of each quarter of year, each product, each country
#     Task 2: Find the number of loyal customers and performance of employees on the first quarter of 1998, based on the policy of company
#     Task 3: Find products which is often ordered together by customers

# %% [markdown]
# The Northwind dataset includes sample data for the following.
#     Suppliers: Suppliers and vendors of Northwind
#     Customers: Customers who buy products from Northwind
#     Employees: Employee details of Northwind traders
#     Products: Product information
#     Shippers: The details of the shippers who ship the products from the traders to the end-customers
#     Orders and Order_Details: Sales Order transactions taking place between the customers & the company

# %%
import pyodbc
import sqlalchemy
import matplotlib

# %%
%load_ext sql

# %%
%sql mssql+pyodbc://sa:12342345@sql

# %% [markdown]
# First of all, we check tables and columns in this database. 

# %%
%%sql
SELECT *
FROM Northwind.information_schema.tables
WHERE TABLE_TYPE='BASE TABLE';

# %%
%%sql
select *
from Northwind.information_schema.columns c join Northwind.information_schema.tables t on c.TABLE_NAME= t.TABLE_NAME
where TABLE_TYPE = 'BASE TABLE';

# %% [markdown]
# Task 1: Track the sale situation of each quarter of year, each product (show top 10), each country

# %%
%%sql
select min(RequiredDate) as start_date, max(RequiredDate) as end_date 
from Northwind.dbo.Orders;

# %%
%%sql
WITH sum_per_quarter AS (
    select year(o.RequiredDate) as year, DATEPART(QUARTER, o.RequiredDate) as quarter, cast(sum(od.UnitPrice*od.Quantity*(1-od.Discount)) as decimal (10,2)) as sum_turnover
    from Northwind.dbo.OrderDetails od join Northwind.dbo.Orders o on od.OrderID=o.OrderID
    group by year(o.RequiredDate), DATEPART(QUARTER, o.RequiredDate)
)
select year, quarter, sum_turnover, cast((sum_turnover/(LAG(sum_turnover) OVER (ORDER BY year, quarter)) -1)*100 as decimal(5,2)) as percent_of_growth
from sum_per_quarter
ORDER BY year, quarter;

# %% [markdown]
# This database records data form 7/1996 to 6/1998. Generally, sales've increased over the quarters. Noticeably, on the first quarter of 1998, sales increased by 49% compared to the last quarter. It seems that sales decreased on the second quarter, but it is reason is that database didn't contain all orders in this quarter. 

# %% [markdown]
# Now, we track sale situation of each product in 1997. First, we need to know how many products the company has.

# %%
%%sql 
SELECT Discontinued, count(*) as count
FROM Northwind.dbo.Products
GROUP BY Discontinued;

# %%
%%sql
WITH top_product AS (
    SELECT sum(od.UnitPrice*od.Quantity*(1-od.Discount)) as sum_revenue, od.ProductID
    FROM Northwind.dbo.OrderDetails od join Northwind.dbo.Orders o on o.OrderID = od.OrderID
    WHERE year(o.RequiredDate)=1997
    GROUP BY od.ProductID
)
SELECT TOP 20 PERCENT p.ProductID, p.ProductName, p.CategoryID, s.SupplierID, s.CompanyName, cast(tp.sum_revenue as decimal(10,2)) as sum, 
    tp.percent_of_total, sum(tp.percent_of_total) OVER (ORDER BY tp.sum_revenue DESC) as cucum_percent, p.Discontinued
FROM (
    SELECT *, CAST(sum_revenue/(SELECT sum(sum_revenue) FROM top_product)*100 as decimal(10,2)) as percent_of_total
    FROM top_product
) as tp 
JOIN Northwind.dbo.Products p ON tp.ProductID = p.ProductID
JOIN Northwind.dbo.Suppliers s ON s.SupplierID = p.SupplierID
ORDER BY tp.sum_revenue DESC;

# %%
%%sql
SELECT CategoryName
FROM Northwind.dbo.Categories 
WHERE CategoryID = 6;

# %% [markdown]
# In this result, we show only top 20 percent of products, which account for more than 50% of revenue in 1997. We notice that there were 3 products in CategoryID 6 (Meat/Poultry) but all of them were discontinued. We should discuss with other departments to find the reason of this discontinuation.

# %%
query= '''
select c.Country, cast(sum(od.UnitPrice*od.Quantity*(1-od.Discount)) as decimal (10,2)) as sum_turnover
from Northwind.dbo.OrderDetails od 
join Northwind.dbo.Orders o on od.OrderID = o.OrderID
join Northwind.dbo.Customers c on c.CustomerID=o.CustomerID
where year(o.RequiredDate) = 1997
group by c.Country
order by sum(od.UnitPrice*od.Quantity*(1-od.Discount)) desc;'''

# %%
result=%sql $query
result

# %% [markdown]
# Task 2: Find the number of loyal customers and performance of employees on the first quarter of 1998, based on the policy of company
# Scenario: Since 1998, Northwind has a policy called "Loyal customers", which is applied for customers in USA, UK and France.
# - If customers bought total value 10000 or above in 1997, they will become "Gold customer" and get discount 5% for all orders in 1998.
# - If customers bought total value 10000 or above in 1997, they will become "Silver customer" and get discount 2% for all orders in 1998.
# Based on this policy, we find number of gold customers, silver customers and find top 3 employees has best performance in the first quarter of 1998. 

# %%
%%sql
WITH customer_type (CustomerID, Value1997,Level1998) AS (
    SELECT o.CustomerID as CustomerID, sum(od.UnitPrice*od.Quantity*(1-od.Discount)) as Value1997, (
        CASE WHEN sum(od.UnitPrice*od.Quantity*(1-od.Discount)) >= 10000 THEN '2. Gold customer'
            WHEN sum(od.UnitPrice*od.Quantity*(1-od.Discount)) >=5000 THEN '1. Silver customer'
            ELSE '0. Normal customer'
        END
    ) as Level1998
    FROM Northwind.dbo.OrderDetails od
    JOIN Northwind.dbo.Orders o ON o.OrderID=od.OrderID 
    JOIN Northwind.dbo.Customers c ON c.CustomerID = o.CustomerID
    WHERE (c.Country='USA' or c.Country= 'UK' or c.Country='France') AND YEAR(o.RequiredDate)=1997
    GROUP BY o.CustomerID
)

SELECT Level1998 AS type_of_customer, count(*) AS Number
FROM customer_type
GROUP BY Level1998;


# %% [markdown]
# The number of gold customers is 3 and the number of silver customers is 9. These amount is acceptable, therefore, this policy haven't to modify. By the end of 1998, we should evaluate its effectiveness, then decide if this policy should be applied for customers all over the world. 

# %%
%%sql
WITH value1997 AS (
    SELECT c.CustomerID as CustomerID, sum(od.UnitPrice*od.Quantity*(1-od.Discount)) as total_of_customer_1997, (
        CASE
        WHEN c.CustomerID IN (SELECT CustomerID FROM Northwind.dbo.Customers WHERE Country IN ('USA', 'UK', 'France')) THEN (
            CASE
            WHEN sum(od.UnitPrice*od.Quantity*(1-od.Discount)) >= 10000 THEN 0.05
            WHEN sum(od.UnitPrice*od.Quantity*(1-od.Discount)) >= 5000 THEN 0.02
            ELSE 0
            END
        )
        ELSE 0
        END
    ) AS customer_discount
    FROM Northwind.dbo.Orders o
    JOIN Northwind.dbo.OrderDetails od ON o.OrderID = od.OrderID 
    JOIN Northwind.dbo.Customers c ON c.CustomerID = o.CustomerID
    WHERE YEAR(o.RequiredDate)= 1997
    GROUP BY c.CustomerID
),
order_total AS (
    SELECT o.OrderID as OrderID, sum(od.UnitPrice*od.Quantity*(1-od.Discount)) as total_of_order
    FROM Northwind.dbo.Orders o JOIN Northwind.dbo.OrderDetails od ON o.OrderID = od.OrderID
    WHERE o.RequiredDate >= '1998-01-01' AND o.RequiredDate < '1998-04-01'
    GROUP BY o.OrderID
),
order_total_after_discount AS (
    SELECT o.OrderID as OrderID, o.EmployeeID as EmployeeID, (
        CASE WHEN o.OrderDate >='1998-01-01' AND o.CustomerID IN (SELECT CustomerID FROM value1997) THEN ot.total_of_order*(1-v.customer_discount) 
        ELSE ot.total_of_order
        END
    ) as after_discount
    FROM Northwind.dbo.Orders o 
    JOIN order_total ot ON o.OrderID= ot.OrderID
    LEFT JOIN value1997 v ON o.CustomerID = v.CustomerID
),
employee_total AS (
    SELECT EmployeeID, sum(after_discount) as total_of_turnover
    FROM order_total_after_discount
    GROUP BY EmployeeID
)
SELECT TOP (3) e.EmployeeID, e.FirstName, e.LastName, e.Title, 
    cast(et.total_of_turnover as decimal(10,2)) as turnover_of_employee, 
    cast((100*et.total_of_turnover/(SELECT sum(total_of_turnover) FROM employee_total)) as decimal(10,2)) as percent_of_quarter
FROM Northwind.dbo.Employees e JOIN employee_total et ON e.EmployeeID = et.EmployeeID
ORDER BY et.total_of_turnover DESC;


# %% [markdown]
# On the first quarter of 1998, top 3 sale employees includes: Janet Leverling (23,4% of total revenue), Margaret Peacock (16,57% of total revenue) and Andrew Fuller - the Vice President. 

# %% [markdown]
# Task 3: Find products which is often ordered together by customers
# Should company have discount when customers buy a "combo", which contains certain product pairs? To answer this question, we need to find which products are often ordered together within a order.

# %%
%%sql
WITH pairs AS (
    SELECT od1.ProductID AS ProductID_1, od2.ProductID AS ProductID_2, od1.OrderID AS OrderID 
    FROM Northwind.dbo.OrderDetails od1 
    JOIN Northwind.dbo.OrderDetails od2 ON od1.OrderID = od2.OrderID
    WHERE od1.ProductID < od2.ProductID
)
SELECT TOP (3) cp.ProductID_1, p1.ProductName as ProductName_1, p1.CategoryID as Category_1, cp.ProductID_2, p2.ProductName as ProductName_2, p2.CategoryID as Category_2, cp.number_of_order
FROM (
    SELECT count(*) AS number_of_order, ProductID_1, ProductID_2
    FROM pairs 
    GROUP BY ProductID_1, ProductID_2
) as cp
LEFT JOIN Northwind.dbo.Products p1 ON cp.ProductID_1 = p1.ProductID
LEFT JOIN Northwind.dbo.Products p2 ON cp.ProductID_2 = p2.ProductID
ORDER BY cp.number_of_order DESC;

# %%
%%sql
SELECT count(*)
FROM Northwind.dbo.Orders;

# %% [markdown]
# The above result show the most common pairs of products. However, the greatest number of order is 8 and very small compared to number of total order (830), then we don't recommend to apply combo discount. 


