import numpy as np
from scipy import sparse

class Recommender():
    def __init__(self, click_mat, deletedProducts, view_mat=None):
        self.click_mat = click_mat
        self.view_mat = view_mat
        self.deletedProducts = deletedProducts
        if view_mat==None:
            print "Initializing Rec with no view mat!"

    def getRecProducts(self, model, userRow, product_feature=None, viewSets=None):
        if self.view_mat!=None:
            ownViewProductSets = set(self.view_mat[userRow].indices)
        elif viewSets != None:
            ownViewProductSets = viewSets
        else:
            print 'View mat is None, viewSets not provided either!!'
            return -1
        ownClickProductSets = set(self.click_mat[userRow].indices)
        totalProducts = self.click_mat.shape[1]
        recProductIDs = []
        if product_feature==None:
            myPreds = np.argsort(model.predict(userRow, np.arange(totalProducts)))[::-1]
        else:
            myPreds = np.argsort(model.predict(userRow, np.arange(totalProducts), item_features=product_feature ))[::-1]
        for a in myPreds:
            if (a not in ownClickProductSets) and (a not in ownViewProductSets):
                recProductIDs.append(a+1)
        recProductIDs += [ p+1 for p in list(ownViewProductSets)]
        # post processing => for example productid 531 is missing!
        recProductIDs = [ x for x in recProductIDs if x not in self.deletedProducts ]
        return recProductIDs

    def testOne(self, newClickIDs, model, product_feature=None):
        old_view_mat = self.view_mat
        old_click_mat = self.click_mat

        if old_view_mat != None:
            self.view_mat = sparse.vstack([ self.view_mat, sparse.csr_matrix(newArr) ])
        
        print 'Getting rec products with model parameters'
        print model.get_params()

        newArr = np.zeros(self.click_mat.shape[1])
        for cid in newClickIDs:
            newArr[cid-1] = 1
        newArr = sparse.csr_matrix(newArr)
        self.click_mat = sparse.vstack([ self.click_mat, newArr ])

        #model = LightFM(no_components=120, loss='warp', max_sampled=20, random_state=0)
        if product_feature==None:
            model.fit(self.click_mat, epochs=20)
        else:
            model.fit(self.click_mat, epochs=20, item_features=product_feature)
        userRow = old_click_mat.shape[0]
        recProductIDs = self.getRecProducts(model, userRow, product_feature, set([]))
        
        self.view_mat = old_view_mat
        self.click_mat = old_click_mat
        return recProductIDs
