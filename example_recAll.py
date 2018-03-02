from datetime import date
import numpy as np
from scipy import sparse
from scipy.spatial.distance import cdist
import gc

from lightfm import LightFM, evaluation
from utils import concatProductFeature

todayDate = date(2017,7,30)
today = todayDate.strftime('%m%d')

# load data
sparse_mat = sparse.load_npz('./user_item_click_sparse_matrix_'+today+'.npz')
sparse_mat_view = sparse.load_npz('./user_item_view_sparse_matrix_'+today+'.npz')
sparse_prod_fea = sparse.load_npz('./products_feature_sparse_'+today+'.npz')
prod_fea_concat = concatProductFeature(sparse_prod_fea, id_weight=1, sparse_weight=0.01)

# set and train model
model = LightFM(no_components=150, loss='warp', max_sampled=20, random_state=0)
model.fit(sparse_mat, epochs=20, item_features=prod_fea_concat)

resultFile = './resultData_0730.csv'

from DataLoader import DataLoader
import config as cfg
DL = DataLoader(cfg)
DL._loadProductData_stream(todayDate)
#DL._loadUserData_stream(todayDate)
DL.loadResultData(resultFile)
ID_to_row_dict = { x: i for i, x in enumerate(DL.resultData.id.unique()) }

from Recommender import Recommender
deletedProducts = set(np.arange(sparse_mat.shape[1])+1).difference(set(DL.productData.id))
RL = Recommender(click_mat=sparse_mat, view_mat=sparse_mat_view, deletedProducts=deletedProducts)

f = open('./rec_0730', 'wb')
for i in DL.resultData.id:
    userRow = ID_to_row_dict[i]
    viewSets = set([ int(x)-1 for x in (DL.resultData.view_products.iloc[userRow].split()) ])
    recProductIDs = RL.getRecProducts(model, userRow, prod_fea_concat, viewSets)
    f.write(str(i)+': '+' '.join([str(x) for x in recProductIDs])+'\n')
    gc.collect()
f.close()

item_fea = model.get_item_representations(features=prod_fea_concat)
total_dist = cdist(item_fea[1], item_fea[1], 'cosine')

f = open('./product_rec_0730', 'wb')
for productID in DL.productData.id:
    similarClickProducts = np.argsort(total_dist[productID-1])[1:21]+1
    f.write(str(productID)+': '+' '.join([str(x) for x in similarClickProducts])+'\n')
    gc.collect()
f.close()
