# -*- coding: utf-8 -*-
import numpy as np
import pandas as pd
from datetime import date, timedelta
from scipy import sparse

import config as cfg
from DataLoader import DataLoader

args_preprocess = False

'''
  To meet the 1G memory upper bound
  DO NOT build sparse_mat and sparse_mat_view at the same time
'''

# read in activities 
start_date = date(2017,3,23)
end_date = (date.today() - timedelta(1))
resultFile = './resultData_0730.csv'

# default is low memory load
DL = DataLoader(cfg ,start_date, date(2017,7,30))
DL.loadData(stream=True)
if args_preprocess:
    DL.genResultData()
    DL.saveData(dataName='resultData', fileName=resultFile)
else:
    DL.loadResultData(resultFile)

todayDate = date(2017,7,30)
today = todayDate.strftime('%m%d')

cold_user = DL.resultData.loc[DL.resultData.click_counts.isnull(), 'id'].values
np.savez('./matrixes/cold_user'+today, cold_user=cold_user)
print "Cold user saved"

exist_user = DL.resultData['id'].values
np.savez('./matrixes/exist_user'+today, exist_user=exist_user)
print "Exist user saved"

sparse_mat = DL.transformToLogMatrix_stream('click_products', unique=True)
sparse.save_npz('./user_item_click_sparse_matrix_'+today+'.npz', sparse_mat)

sparse_mat_view = DL.transformToLogMatrix_stream('view_products', unique=False)
sparse.save_npz('./user_item_view_sparse_matrix_'+today+'.npz', sparse_mat_view)

# save products features
newProductData = DL.transformProductMatrix(colWeights=[1, 1, 1])
sparse_prod_fea = sparse.csr_matrix(newProductData.drop('index', 1).values)
sparse.save_npz('./products_feature_sparse_'+today+'.npz', sparse_prod_fea)
del newProductData
