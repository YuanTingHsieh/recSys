'''
Example of rec products to a new user with 3 click logs
'''
from lightfm import LightFM, evaluation

from scipy import sparse
import numpy as np
from utils import make_train, concatProductFeature
from datetime import date

todayDate = date(2017,7,30)
today = todayDate.strftime('%m%d')

# load data
sparse_mat = sparse.load_npz('./user_item_click_sparse_matrix_'+today+'.npz')
sparse_mat_view = sparse.load_npz('./user_item_view_sparse_matrix_'+today+'.npz')
sparse_prod_fea = sparse.load_npz('./products_feature_sparse_'+today+'.npz')
prod_fea_concat = concatProductFeature(sparse_prod_fea, id_weight=1, sparse_weight=0.01)

X_train, X_test, indices = make_train(sparse_mat, 0.2)

'''
  test the performance of model
'''
model = LightFM(no_components=100, loss='warp', max_sampled=20, random_state=0)
model.fit(X_train, epochs=20)
test_auc = evaluation.auc_score(model, sparse_mat, train_interactions=X_train)

from DataLoader import DataLoader
import config as cfg
DL = DataLoader(cfg)
DL._loadProductData_stream(todayDate)

from Recommender import Recommender
deletedProducts = set(np.arange(sparse_mat.shape[1])+1).difference(set(DL.productData.id))
RL = Recommender(click_mat=sparse_mat, view_mat=sparse_mat_view, deletedProducts=deletedProducts)

# test a new user with newClickIDs
newClickIDs = [1966, 3118, 1158]
recID = RL.testOne(model=LightFM(no_components=100, loss='warp', max_sampled=20, random_state=0),\
 newClickIDs=newClickIDs)
recID_2 = RL.testOne(model=LightFM(no_components=150, loss='warp', max_sampled=20, random_state=0),\
 newClickIDs=newClickIDs, product_feature=prod_fea_concat)

# view results
#DL.productData[ [x in sparse_mat[userRow].indices+1 for x in DL.productData.id ]  ][['_source.name', '_source.tags']]
DL.productData[ [x in newClickIDs for x in DL.productData.id ]  ][['_source.name', '_source.tags']]
DL.productData[ [x in recID[0:10] for x in DL.productData.id ]  ][['_source.name','_source.tags']]
