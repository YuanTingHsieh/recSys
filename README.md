# Recommendation System

## Overview
- User enter website -> Check if user is cold or not (based on click results)
  - User not cold -> **Matrix Factorization (lightfm)** or correlation or libFFM results
  - User cold -> check if we have user preferences or not
    - Have user preferences -> 
      1. **Use preferences to find match products (based on matched preferences, scoring on tags, themes, etc.) -> elastic search built in**
      2. Use preferences to find similar users
      3. Recommend popular {click/view/like/rent} products based on preferences
    - Not have preferences -> 
      1. **Recommend top click products**
      2. Recommend top {view/like/rent} products
- Final step: move **views > 1** products to the bottom

## Structure of this directory
- config.py: Configuration file
- DataLoader.py: Provides interfaces to load, tranform and update required data
- Recommender.py: Provides interfaces to get recommendation products and test one new user
- utils.py: Contains some helper functions
- Some examples that are self-explained

## For product show page
- Show **You might also like** by [ElasticSearch](https://www.elastic.co/cn/) tf-idf-like scoring on product's name, tags etc.
- Show **Others click on** by scoring based on product embedding vector obtained by MF (example_getSimProduct.py)

## Brief Introduction of Matrix Factorization
Say we have a user-product matrix R which contains the click counts

it is of dimension |U|x|D|

while |U| is the number of user and |D| is the number of product

MF aims at divide R to be two matrix  P (|U| x K) and Q (|D| x K)

where K is the latent dimension we set

And satisfy that R ~ R_hat = P x Q'

So every p (a 1xK vector) can be think of a user's embedding vector

and every q (a 1xK vector) is the product's embedding vector，

multiply someone's p with a product's q yields the score that he/she could click a product

To do the recommendation, for each person, we count the score for every product

And we sort the score and recommend the highest ones

An intuition behind this is we do dimension reduction from a sparse matrix to capture the underlying correspondence between each user and products

## Initialize the environment
1. Make sure the following directories exist
  - user_act_json
  - raw_data
  - matrixes
  - user_behavior
2. Make sure to check the dependencies
  - Python Libraries: numpy, pandas, ijson, [lightfm](https://github.com/lyst/lightfm)
  - Other software: [libmf](https://www.csie.ntu.edu.tw/~cjlin/libmf/)
3. Download user activies from kibana from 3/23 to yesterday (see `user_act_json/downloadAllAct.sh`)
4. Download user, product data from kibana of today (run `bash dailywork.sh`)
5. Download JJ, QQ, KK, AA user data from metabase of today (manual)
6. Download {tags, themes, colors, shapes, categories} id <--> name match file from metabase (manual)

## Total Process Procedure
1. Prepare data and user click matrix (examples: prepareData.py)
2. Recommending products to user based on his/her click results! (examples: recOne.py)
3. Find similar products for a given productID (examples: getSimProduct.py)

## For low memory implementation
- Use [ijson](https://pypi.python.org/pypi/ijson) instead of json
- Calling `gc.collect()` do mitigate the memory leak problem
- Use more disk instead of RAM, trade speed for space
