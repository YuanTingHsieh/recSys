#!/bin/bash
# using yesterday to ensure the integrity of kibana records
today=$(date -d "yesterday 13:00" +%m%d)
myAWS="https://elastic:YvPJG3euqQ6sEdjk4IRYurj4@6ecabab46d736114e67db452395fbd9f.us-east-1.aws.found.io:9243"
actJson="./user_act_json/${today}.json"
userJson="./raw_data/users_${today}.json"
productJson="./raw_data/products_${today}.json"
echo "${myAWS}/user_activity_2017${today}"

# not checking in the script
# make sure the following directories exist before running
# user_act_json, user_act_csv, raw_data
# user_behavior, matrixes, mf_files

# daily script to update users profile and recommendation models
# 1. log in kibana, run in dev tools, set the download window 
# (don't know how to automate this!)
# PUT user_activity_2017xxxx/_settings
# {
#   "index": {
#     "max_result_window": 1000000
#   }
# }
#
# 2. download user activities from kibana
file="${actJson}"
if [ -e "$file" ]
then
    echo "${file} already exists, skipping updates"
else
    curl -XGET "${myAWS}/user_activity_2017${today}/_search?pretty" -H 'Content-Type: application/json' -d'
    {
      "query": {
        "bool": {
          "must": {
            "match_all": {}
          }
        }
      },
      "size": 1000000,
      "from": 0,
      "sort": {
        "created_at": "DESC"
      },
      "timeout": "11s"
    }
    ' >> $file
fi

# 3. download product data from kibana
file="${productJson}"
if [ -e "$file" ]
then
    echo "${file} already exists, skipping updates"
else
    curl -XGET "${myAWS}/products_production/_search?pretty" -H 'Content-Type: application/json' -d'
    {
      "query": {
        "bool": {
          "must": {
            "match_all": {}
          }
        }
      },
      "size": 1000000,
      "from": 0,
      "sort": {
        "created_at": "DESC"
      },
      "timeout": "11s"
    }
    ' >> $file
fi

# 3. download user profile from kibana
file="${userJson}"
if [ -e "$file" ]
then
    echo "${file} already exists, skipping updates"
else
    curl -XGET "${myAWS}/users_production/_search?pretty" -H 'Content-Type: application/json' -d'
    {
      "query": {
        "bool": {
          "must": {
            "match_all": {}
          }
        }
      },
      "size": 1000000,
      "from": 0,
      "sort": {
        "created_at": "DESC"
      },
      "timeout": "11s"
    }
    ' >> $file
fi
