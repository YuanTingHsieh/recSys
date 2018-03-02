import numpy as np
import pandas as pd
import subprocess as sub
import random
from scipy import sparse

def myRunCommand(command):
    print 'Running '+command
    prog = sub.Popen(command, stdout=sub.PIPE, stderr=sub.PIPE, shell=True)
    out, err = prog.communicate()
    print out

def myAP(truth, preds):
    sorted_preds = np.sort(preds)[::-1]
    levels = np.unique(preds)
    precisions = []
    numTruth = len(truth.nonzero()[0])
    hitNum = 0
    correct = 0.0
    wrong = 0.0
    for i, a in enumerate(np.argsort(preds)[::-1]):
        if hitNum==numTruth:
            break
        if a in truth.nonzero()[0]:
            # hit
            correct += 1
            hitNum += 1
            precisions.append(correct/(correct+wrong))
            #print correct/(correct+wrong)
        else:
            wrong += 1
    return np.mean(precisions)

def totalAP(click_mat, pref_mat, productIDs, productID_tag):
    total_AP = []
    total_frac = []
    for clickID in click_mat.sum(axis=1).nonzero()[0][0:50]:
        new_vector = np.zeros(max(productIDs))
        for productID in productIDs:
            for productTag in productID_tag[productID]:
                new_vector[int(productID)-1] += float(pref_mat[clickID, tag_dict[productTag]])/len(productID_tag[productID]) 
        ap = myAP(click_mat[clickID], new_vector)
        frac = float(len(np.unique(new_vector)))/len(new_vector.nonzero()[0])
        print "AP is ", ap, " Frac is ", frac, " Click pro ", len(click_mat[clickID].nonzero()[0])
        total_AP.append(ap)
        total_frac.append(frac)
    print np.mean(total_AP)

def make_train(ratings, pct_test = 0.2):
    '''
    This function will take in the original user-item matrix and "mask" a percentage of the original ratings where a
    user-item interaction has taken place for use as a test set. The test set will contain all of the original ratings, 
    while the training set replaces the specified percentage of them with a zero in the original ratings matrix. 
    
    parameters: 
    
    ratings - the original ratings matrix from which you want to generate a train/test set. Test is just a complete
    copy of the original set. This is in the form of a sparse csr_matrix. 
    
    pct_test - The percentage of user-item interactions where an interaction took place that you want to mask in the 
    training set for later comparison to the test set, which contains all of the original ratings. 
    
    returns:
    
    training_set - The altered version of the original data with a certain percentage of the user-item pairs 
    that originally had interaction set back to zero.
    
    test_set - A copy of the original ratings matrix, unaltered, so it can be used to see how the rank order 
    compares with the actual interactions.
    
    user_inds - From the randomly selected user-item indices, which user rows were altered in the training data.
    This will be necessary later when evaluating the performance via AUC.
    '''
    test_set = ratings.copy() # Make a copy of the original set to be the test set. 
    test_set[test_set != 0] = 1 # Store the test set as a binary preference matrix
    training_set = ratings.copy() # Make a copy of the original data we can alter as our training set. 
    nonzero_inds = training_set.nonzero() # Find the indices in the ratings data where an interaction exists
    nonzero_pairs = list(zip(nonzero_inds[0], nonzero_inds[1])) # Zip these pairs together of user,item index into list
    #random.seed(0) # Set the random seed to zero for reproducibility
    num_samples = int(np.ceil(pct_test*len(nonzero_pairs))) # Round the number of samples needed to the nearest integer
    samples = random.sample(nonzero_pairs, num_samples) # Sample a random number of user-item pairs without replacement
    user_inds = [index[0] for index in samples] # Get the user row indices
    item_inds = [index[1] for index in samples] # Get the item column indices
    training_set[user_inds, item_inds] = 0 # Assign all of the randomly chosen user-item pairs to zero
    training_set.eliminate_zeros() # Get rid of zeros in sparse array storage after update to save space
    return training_set, test_set, list(set(user_inds)) # Output the unique list of user rows that were altered  

# this does not work well
def vectorizeUserData(originalData, colWeights):
    listCols = [col for col in originalData.columns if 'preference' in col]
    listCols = ['_source.general_type'] + listCols
    df = originalData.copy(deep=True)
    df['_source.general_type'] = df['_source.general_type'].apply(lambda x: [x])
    for j, col in enumerate(listCols):
        print 'Vectorizing ', col
        pref_count = Counter()
        for val_list in df[col].values:
            pref_count.update(val_list)
        print pref_count
        pref_dict = { x: i for i,x in enumerate(pref_count.keys()) }
        matrix = np.zeros((len(df), len(pref_dict.keys())))
        for i, val_list in enumerate(df[col].values):
            for val in val_list:
                matrix[i, pref_dict[val]] = 1
        sub_df = pd.DataFrame(matrix, columns=[col+'_'+x for x in sorted(pref_dict, key=pref_dict.get)])
        df = pd.concat([df, sub_df], axis=1)
        df = df.drop(col, axis=1)
    return df

def testAllClickX(DataLoader, Recommender, clickNum, model, product_feature=None, testNum=5, recNum=10):
    '''
    Test the top {recNum} recommendation results of {testNum} users who have clicked {clickNum} products
    Output a readable dataframe
    '''
    click_X_userRows = np.where(Recommender.click_mat.sum(axis=1)==clickNum)[0][0:testNum]
    toDumpAll = []
    for userRow in click_X_userRows:
        toDump = []
        recProductIDs = Recommender.getRecProducts(model, userRow, product_feature)
        toDump.append(DataLoader.productData[ [x in Recommender.click_mat[userRow].indices+1 for x in DataLoader.productData.id ]  ][['id','_source.name', '_source.tags']])
        toDump.append(DataLoader.productData[ [x in recProductIDs[0:recNum] for x in DataLoader.productData.id ]  ][['id','_source.name','_source.tags']])
        df = pd.concat(toDump)
        df['userID'] = DataLoader.mergeData.id.iloc[userRow]
        df['userRow'] = userRow
        df['type'] = ['click']*clickNuml+['rec']*recNum
        toDumpAll.append(df)
    toDumpAll = pd.concat(toDumpAll)
    return toDumpAll

def concatProductFeature(product_feature, id_weight=1, sparse_weight=0.01):
    # concat eye matrix with products cate, color, tag features
    eye = sparse.eye(sparse_prod_fea.shape[0], sparse_prod_fea.shape[0]).tocsr()
    eye = eye*id_weight
    prod_fea_concat = sparse.hstack((eye, sparse_prod_fea*sparse_weight))
    prod_fea_concat = prod_fea_concat.tocsr().astype(np.float32)
    return prod_fea_concat
