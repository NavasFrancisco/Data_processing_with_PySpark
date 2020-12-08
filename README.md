# Data_processing_with_PySpark
Course information can be found at https://www.notion.so/8285d1c9593d4699b9683fddf8518c94?v=5bae3fbfdd934e518f68d6a503f4f203&p=853286980e5d4a03a9d643e6b030f8ab

Results.ypnb contains my solution to the problem defined below.

Data for the exercise is https://drive.google.com/file/d/1kCXnIeoPT6p9kS_ANJ0mmpxlfDwK1yio/view

# The Dataset

Let’s describe briefly the dataset that we are going to use: i**t consists of three tables coming from the database of a shop, with products, sales and sellers.** Data is available in Parquet files that you can download at the link below. **Note that to exploit the exercises 100% you’ll need to read from the files provided!**

The following diagram shows how the tables can be connected: 

![https://miro.medium.com/max/792/1*wA4xJu3LMcm_vR5pFJkLpA.png](https://miro.medium.com/max/792/1*wA4xJu3LMcm_vR5pFJkLpA.png)

## **Sales Table**

Each row in this table is an order and every order can contain only one product. Each row stores the following fields:

- `order_id`: The **order ID**
- `product_id`: The single product sold in the order. **All orders have exactly one product**)
- `seller_id`: The selling employee ID that sold the product
- `num_pieces_sold`: The **number of units sold for the specific product** in the order
- `bill_raw_text`: A string that represents the **raw text of the bill** associated with the order
- `date`: **The date** of the order.

## Products Table

Each row represents a **distinct product**. The fields are:

- `product_id`: The product ID
- `product_name`: The product name
- `price`: The product price

## Sellers Table

This table contains the **list of all the sellers**:

- `seller_id`: The seller ID
- `seller_name`: The seller name
- `daily_target`: The number of items (**regardless of the product type**) that the seller needs to hit his/her quota. For example, if the daily
target is 100,000, the employee needs to sell 100,000 products he can
hit the quota by selling 100,000 units of product_0, but also selling
30,000 units of product_1 and 70,000 units of product_2

# Exercises

The best way to exploit the exercises below is to **download the data and implement a working code that solves the proposed problems**, ideally ****in a distributed environment! I would advise doing so before reading the solutions that are available at the end of the page!

Tip: I built the dataset to allow working on a single machine: **when writing the code, imagine what would happen with a dataset 100 times bigger.**

Even if you’d know how to solve them, **my advice is not to skip the warm-up questions!** (if you know Spark they’ll take a few seconds).

If you are going to do the exercise with Python, you‘ll need the following packages:

## Warm-up #1

```
1) Find out how many orders, how many products and how many sellers are in the data.How many products have been sold at least once? Which is the product contained in more orders?
===========Create the Spark session using the following 
codespark = SparkSession.builder \
    .master("local") \
    .config("spark.sql.autoBroadcastJoinThreshold", -1) \
    .config("spark.executor.memory", "500mb") \
    .appName("Exercise1") \
    .getOrCreate()
```

## Warm-up #2

```
How many distinct products have been sold in each day?
===========Create the Spark session using the following 
codespark = SparkSession.builder \
    .master("local") \
    .config("spark.sql.autoBroadcastJoinThreshold", -1) \
    .config("spark.executor.memory", "500mb") \
    .appName("Exercise1") \
    .getOrCreate()
```

## Exercise #1

```
What is the average revenue of the orders?
```

## Exercise #2

```
For each seller, what is the average % contribution of an order to the seller's daily quota?
# Example
If Seller_0 with `quota=250` has 3 orders:
Order 1: 10 products sold
Order 2: 8 products sold
Order 3: 7 products sold
The average % contribution of orders to the seller's quota would be:
Order 1: 10/105 = 0.04
Order 2: 8/105 = 0.032
Order 3: 7/105 = 0.028
Average % Contribution = (0.04+0.032+0.028)/3 = 0.03333
```

## Exercise #3

```
Who are the second most selling and the least selling persons (sellers) for each product? 
Who are those for product with `product_id = 0`
```

## Exercise #4

```
Create a new column called "hashed_bill" defined as follows:
- if the order_id is even: apply MD5 hashing iteratively to the bill_raw_text field, 
once for each 'A' (capital 'A') present in the text. 
E.g. if the bill text is 'nbAAnllA',
 you would apply hashing three times iteratively (only if the order number is even)- 
- if the order_id is odd: apply SHA256 hashing to the bill textFinally, 
check if there are any duplicate on the new column
```
