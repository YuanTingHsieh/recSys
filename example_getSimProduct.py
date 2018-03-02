'''
  Get similar click products and put on products page!
  (Everyday or every week work)
'''
from scipy.spatial.distance import cdist
from scipy import sparse
from lightfm import LightFM

from utils import concatProductFeature
from datetime import date

todayDate = date(2017,7,30)
today = todayDate.strftime('%m%d')

# load data
sparse_mat = sparse.load_npz('./user_item_click_sparse_matrix_'+today+'.npz')
sparse_mat_view = sparse.load_npz('./user_item_view_sparse_matrix_'+today+'.npz')
sparse_prod_fea = sparse.load_npz('./products_feature_sparse_'+today+'.npz')
prod_fea_concat = concatProductFeature(sparse_prod_fea, id_weight=1, sparse_weight=0.01)

model = LightFM(no_components=150, loss='warp', max_sampled=20, random_state=0)
model.fit(sparse_mat, epochs=20, item_features=prod_fea_concat)

item_fea = model.get_item_representations(features=prod_fea_concat)
total_dist = cdist(item_fea[1], item_fea[1], 'cosine')

productID = 3088
similarClickProducts = np.argsort(total_dist[productID-1])[1:21]+1


# view results
from DataLoader import DataLoader
import config as cfg
DL = DataLoader(cfg)
DL._loadProductData_stream(todayDate)

DL.productData[ [x in [productID] for x in DL.productData.id ]  ][['_source.name', '_source.tags']]
DL.productData[ [x in similarClickProducts for x in DL.productData.id ]  ][['_source.name','_source.tags']]
