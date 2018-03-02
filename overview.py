'''
Simulating all circumstances of one user recommendation
'''
import numpy as np
from datetime import date

userID = ?????
newDay = date(2017,7,16)
today = newDay.strftime('%m%d')
cold_list = np.load('./matrixes/cold_user'+today+'.npz')['cold_user']
cold_set = set(cold_list)
exist_list = np.load('./matrixes/exist_user'+today+'.npz')['exist_user']
existID = set(exist_list)
userID_row = { userID: i for i, userID in enumerate(exist_list) }
# if new user is operating
if userID not in existID:
    #subData = ???
    # find similar user by preferences?? 
    userRow = findSimilarUser()
else:
    userRow = userID_row[userID]
