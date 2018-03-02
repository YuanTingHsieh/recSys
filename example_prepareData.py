# -*- coding: utf-8 -*-
'''
This code consume lots of RAM!!! 
Better have RAM >= 16GB
Or run prepareData_lowmem.py instead
'''
import numpy as np
import pandas as pd
from datetime import date, timedelta
from scipy import sparse

import config as cfg
from DataLoader import DataLoader

args_preprocess = False

# read in activities 
start_date = date(2017,3,23)
end_date = (date.today() - timedelta(1))
resultFile = './mergeData_0716.csv'

# default is low memory load
DL = DataLoader(cfg ,start_date, date(2017,7,16))
DL.loadData(stream=False)
if args_preprocess:
    DL.preprocessAct()
    DL.mergeUserAct()
    DL.saveData(dataName='resultData', fileName=resultFile)
else:
    DL.loadResultData(resultFile)

newDay = date(2017,7,16)
today = newDay.strftime('%m%d')

cold_user = DL.resultData.loc[DL.resultData.click_counts.isnull(), 'id'].values
np.savez('./matrixes/cold_user'+today, cold_user=cold_user)
print "Cold user saved"

exist_user = DL.resultData['id'].values
np.savez('./matrixes/exist_user'+today, exist_user=exist_user)
print "Exist user saved"


user_item_click_matrix = DL.transformToLogMatrix('click_products', unique=True)
sparse_mat = sparse.csr_matrix(user_item_click_matrix)
sparse.save_npz('./user_item_click_sparse_matrix_'+today+'.npz', sparse_mat)
del user_item_click_matrix

user_item_view_matrix = DL.transformToLogMatrix('view_products', unique=False)
sparse_mat_view = sparse.csr_matrix(user_item_view_matrix)
sparse.save_npz('./user_item_view_sparse_matrix_'+today+'.npz', sparse_mat_view)
del user_item_view_matrix

# save products features
newProductData = DL.transformProductMatrix(colWeights=[1, 1, 1])
sparse_prod_fea = sparse.csr_matrix(newProductData.drop('index', 1).values)
sparse.save_npz('./products_feature_sparse_'+today+'.npz', sparse_prod_fea)
del newProductData
