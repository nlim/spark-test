You are productizing a batch machine learning pipeline that makes product
recommendations. Transaction data from all customer stores is available as an
input in the following format:

    customer_uuid     date         store_id product_sku  item_count usd_total
    34a1a589-cc39-... 2018-01-01.. 18       12421        1          15.25

In addition, product catalog data is also available:

    product_sku  product_category
    12421-345                Food
    10343-124               Latte

Design a batch data processing pipeline that runs nightly and computes features
for the ML model to train on. Specifically, for every customer, calculate the
following:

1. *weekly_food_purchases*, weekly_nonfood_purchases: for the past 8 week period
   (56 days), how many food and non-food items has each customer purchased per
   week (on average)

2. *weekly_spend*: for the past 8 week period (56 days), how much money has each
   customer spend per week (on average)

For a bonus task, compute *weekly_visits*, how many times has each customer
visited the store per week (on average).
