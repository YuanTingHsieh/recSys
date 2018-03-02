# Recommendation System

## Overview
- User enter website -> Check if user is cold or not (based on click results)
  - User not cold -> **Matrix Factorization (lightfm)** or correlation or libFFM results
  - User cold -> check if we have user preferences or not
    - Have user preferences -> 
      1. **Use preferences to find match products (based on matched preferences, scoring on tags, themes, etc.) -> elastic search built in**
      2. Use preferences to find similar users
      3. Recommend popular {act} products based on preferences
    - Not have preferences -> 
      1. **Recommend top click products**
      2. Recommend top {view/like/rent} products
- Final step: move **views > 1** products to the bottom

## Structure of this directory
- config.py: Configuration file
- DataLoader.py: Provides interfaces to load, tranform and update required data
- Recommender.py: Provides interfaces to get recommendation products and test one new user
- utils.py: Contains some helper functions
- Some examples that are self-explainable

## For product show page
- Show **你可能還會喜歡** by [ElasticSearch](https://www.elastic.co/cn/) tf-idf-like scoring on product's name, tags etc.
- Show **其他人也點了** by scoring based on product embedding vector obtained by MF (example_getSimProduct.py)

## Brief Introduction of Matrix Factorization
顧客對產品的點擊次數 是一個矩陣 R (dimension |U|x|D|)

|U|是總共顧客的數量，|D|是總共產品的數量

MF是把這矩陣拆成 兩個矩陣 P (|U| x K) 和 Q (|D| x K) 相乘，K是我們自己設的維度

R 約等於 R_hat = P x Q

然後每一個 p (a 1xK vector) 就是代表那個user的embedding vector，

每一個 q (a  1xK vector) 就是代表那個product的embedding vector，

將某個人的 p 乘上一個產品的 q 即得出他點擊該產品的可能性，也就是分數，

對每個人，把這些分數sorting好，高分在前、低分在後，這樣子去推薦。

而從|U|和|D|的dimension降維到K，

中間所獲得資訊，

就有點像是哪些user的行為相像的感覺。

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
