# data loader
from datetime import date, timedelta
import pandas as pd
import numpy as np
import codecs
import os
import ijson
from collections import Counter
from scipy import sparse
import gc

class DataLoader():
    def __init__(self, configFile, start_date=date(2017,3,23), end_date=date.today()-timedelta(1)):
        self.cfg = configFile
        self.start_date = start_date
        self.end_date = end_date
        self.actData = pd.DataFrame()
        self.userData = pd.DataFrame()
        self.productData = pd.DataFrame()
        self.resultData = pd.DataFrame()
        self.tag_dict = {}
        self.userTagsDF = pd.DataFrame()

    def preprocessAct(self, start_date=None, end_date=None):
        '''
        Extract activity pairs (user <--> products) from start_date to end_date
        Read act json from ACTIVITY_DIRECTORY of config.py
        Cost 3.5~4.2% memories for just one day!!
        '''
        userIDs = []
        actable_types = []
        productIDs = []
        source_types = []
        if start_date == None:
            start_date = self.start_date
        if end_date == None:
            end_date = self.end_date
        print 'Total processing from ', start_date.strftime("%Y%m%d"), ' to ', end_date.strftime("%Y%m%d")
        for day in pd.date_range(start_date, end_date):
            print 'Processing ', day
            filename = os.path.join(self.cfg.ACTIVITY_DIRECTORY, day.strftime("%m%d")+'.json')
            try:
                actJson = pd.read_json(filename)
            except:
                print filename, ' does not exists!'
                continue
            for record in actJson['hits']['hits']:
                userIDs.append(record['_source']['user_id'])
                actable_types.append(record['_source']['actable_type'])
                productIDs.append(record['_source']['product_id'])
                source_types.append(record['_source']['source_type'])
            del actJson
        total_data = pd.DataFrame(data={'_source.user_id': userIDs, '_source.actable_type': actable_types,'_source.product_id': productIDs,'_source.source_type':source_types})
        total_data.sort_values(by='_source.user_id', inplace=True)
        total_data['_source.product_id'] = total_data['_source.product_id'].apply(str)
        del userIDs, actable_types, productIDs, source_types
        # remove source_type with collections#index:special
        total_data = total_data.loc[total_data['_source.source_type']!='collections#index:special']
        self.actData = total_data

    def preprocessAct_stream(self, day):
        '''
        Extract activity pairs (user <--> products) of day
        Read act json from ACTIVITY_DIRECTORY of config.py
        Cost X% memories for one day
        '''    
        print 'Processing ', day
        if (day.strftime("%m%d") == '0409'):
            return -1
        records = []
        filename = os.path.join(self.cfg.ACTIVITY_DIRECTORY, day.strftime("%m%d")+'.json')
        try:
            for value in ijson.items(open(filename, 'rb'), 'hits.hits.item'):
                one_record = {}
                one_record['_source.user_id'] = value['_source']['user_id']
                one_record['_source.actable_type'] = value['_source']['actable_type']
                one_record['_source.product_id'] = value['_source']['product_id']
                one_record['_source.source_type'] = value['_source']['source_type']
                records.append(one_record)
        except:
            print filename, ' does not exists!'
            return -1
        data = pd.DataFrame(records)
        data.sort_values(by='_source.user_id', inplace=True)
        data['_source.product_id'] = data['_source.product_id'].apply(str)
        del records
        # remove source_type with collections#index:special
        data = data.loc[data['_source.source_type']!='collections#index:special']
        # save data
        fileName = './streamAct/'+day.strftime("%m%d")+'.csv'
        f = codecs.open(fileName, 'wb', encoding='utf-8')
        f.write(data.to_csv())
        f.close()
        self.actData = data
        return 0

    def mergeUserAct(self):
        '''
        Merge actData to userdata
        Output columns: user_id, user_type, user_preferences, 
        click counts, view counts, click products, view products
        '''
        # for not unique counts => total_data.groupby('_source.user_id').size().rename('log_counts').reset_index()
        # for unique counts => total_data.groupby('_source.user_id')['_source.product_id'].nunique().rename('log_counts').reset_index()
        total_data = self.actData
        #total_logs = total_data.groupby('_source.user_id')['_source.product_id'].nunique().rename('log_counts').reset_index()
        total_views = total_data.loc[total_data['_source.actable_type']=='Product::View'].groupby('_source.user_id')['_source.product_id'].nunique().rename('view_counts').reset_index()
        total_clicks = total_data.loc[total_data['_source.actable_type']=='Product::Click'].groupby('_source.user_id')['_source.product_id'].nunique().rename('click_counts').reset_index()
        total_view_products = total_data.loc[total_data['_source.actable_type']=='Product::View'].groupby('_source.user_id')['_source.product_id'].apply(lambda x: ' '.join(x)).rename('view_products').reset_index()
        total_click_products = total_data.loc[total_data['_source.actable_type']=='Product::Click'].groupby('_source.user_id')['_source.product_id'].apply(lambda x: ' '.join(x)).rename('click_products').reset_index()
        #resultData = self._mergeLeft(self.userData, total_logs, 'id', '_source.user_id')
        #resultData = self._mergeLeft(resultData, total_views, 'id', '_source.user_id')
        resultData = self._mergeLeft(self.userData, total_views, 'id', '_source.user_id')
        resultData = self._mergeLeft(resultData, total_clicks, 'id', '_source.user_id')
        resultData = self._mergeLeft(resultData, total_view_products, 'id', '_source.user_id')
        resultData = self._mergeLeft(resultData, total_click_products, 'id', '_source.user_id')
        ## keep only users with records or created after 2017-03-23
        ## newData = resultData[(resultData.log_counts.notnull()) | pd.to_datetime(resultData.created_at) > datetime.datetime.fromtimestamp(time.mktime(time.strptime('2017-03-23', '%Y-%m-%d')))]
        self.resultData = resultData

    def loadResultData(self, fileName):
        '''
        0.6% => 2.2% consume 1.6% memories
        dtype specify is useless
        '''
        self.resultData = pd.read_csv(fileName, encoding='UTF8', index_col=0)

    def saveData(self, dataName, fileName):
        f = codecs.open(fileName, 'wb', encoding='utf-8')
        if dataName == 'resultData':
            f.write(self.resultData.to_csv())
        elif dataName == 'userData':
            f.write(self.userData.to_csv())
        elif dataName == 'productData':
            f.write(self.productData.to_csv())
        elif dataName == 'actData':
            f.write(self.actData.to_csv())
        else:
            print 'Unrecognized dataName: ', dataName
        f.close()

    def loadData(self, dataName='All', stream=True, end_date=None):
        '''
        Load necessary data, wrapper function
        Stream Verison: mem cost 2.1% of 16GB
        '''
        if stream == True:
            if dataName == 'All':
                self._loadUserData_stream(end_date)
                self._loadProductData_stream(end_date)
            elif dataName == 'userData':
                self._loadUserData_stream()
            elif dataName == 'productData':
                self._loadProductData_stream()
            elif dataName == 'actData':
                print 'actData not supported'
            else:
                print 'Unrecognized dataName: ', dataName            
        else:
            if dataName == 'All':
                self._loadUserData(end_date)
                self._loadProductData(end_date)
            elif dataName == 'userData':
                self._loadUserData()
            elif dataName == 'productData':
                self._loadProductData()
            elif dataName == 'actData':
                print 'actData not supported'
            else:
                print 'Unrecognized dataName: ', dataName

    def updateData(self, newDay):
        '''
        - update resultData based on new day's record
        - userData and productData should be newest
        - while we do not want to run processAct for total days again
        - For 1G RAM, approximately can update data for 2,3 days in one execution
        '''
        if self.resultData.empty:
            print 'resultData not load yet!'
            return -1
        oldResultData = self.resultData.copy()
        if self.preprocessAct_stream(newDay)==-1:
            del oldResultData
            return -1
        self.mergeUserAct()
        #activities = ['view', 'click', 'like', 'addToCart', 'removeFromCart']
        activities = ['view', 'click']
        colsToAdd = [ x+'_counts' for x in activities ]
        colsToJoin = [ x+'_products' for x in activities ]
        self.resultData[colsToAdd] = self.resultData[colsToAdd].add(oldResultData[colsToAdd], fill_value=0)
        self.resultData[colsToJoin] = self.resultData[colsToJoin].add(' ', fill_value='')
        self.resultData[colsToJoin] = self.resultData[colsToJoin].add(oldResultData[colsToJoin], fill_value='')
        for col in colsToJoin:
            self.resultData[col] = self.resultData[col].apply(lambda x: ' '.join(x.split()))
        del oldResultData
        gc.collect()
        return 0

    def genResultData(self, start_date=None, end_date=None):
        '''
        Streaming (memory efficient) version to generate result data
        cost 1G from 3/23 ~ 4/4
        '''
        if start_date == None:
            start_date = self.start_date
        if end_date == None:
            end_date = self.end_date
        print 'Total processing from ', start_date.strftime("%Y%m%d"), ' to ', end_date.strftime("%Y%m%d")
        # initialize resultData
        self.preprocessAct_stream(start_date)
        self.mergeUserAct()
        for day in pd.date_range(start_date+timedelta(1), end_date):
            if self.updateData(day)==-1:
                print 'Update of '+day.strftime('%m%d')+' failed'
            else:
                print 'Successfully update result data with '+day.strftime('%m%d')
        print 'Gen result data completed'

    def countProductTags(self, activity_list=['view','click']):
        '''
        Count the logs of 'act' on each 'tags'
        '''
        # total_activities = ['view','click','like','addToCart','removeFromCart','rent']
        userTagsDF = pd.concat([self._countProductTags(activity) for activity in activity_list ], axis=1)
        self.userTagsDF = userTagsDF

    def transformToLogMatrix(self, actCol, unique=True, userData=None, productData=None):
        '''
        Tranform merged user-act data to user-product click/view matrix
        Output a dense matrix
        Unique == True means 0/1, otherwise will use all counts
        For example, user 1 view_products 453, 453 
          => unique==True  yields 1 at mat[1,453]
          => unique==False yields 2 at mat[1,453]
        '''
        if userData == None:
            userData = self.userData
        if productData == None:
            productData = self.productData
        logs = self.resultData
        ID_to_row_dict = { x: i for i, x in enumerate(userData.id.unique()) }
        user_item_matrix = np.zeros(shape=( len(userData.id.unique()), max(productData.id)) )
        for i, userID in enumerate(userData.id):
            actProducts = logs.loc[logs.id==userID, actCol].values[0]
            try:
                np.isnan(actProducts)
            except:
                if unique==True:
                    # care click or not, just like advertisement?
                    for productID in set(actProducts.split()):
                        user_item_matrix[ID_to_row_dict[userID], (int(productID)-1)] += 1
                elif unique==False:
                    for productID in actProducts.split():
                        user_item_matrix[ID_to_row_dict[userID], (int(productID)-1)] += 1
        
        print "Construction log matrix complete "
        return user_item_matrix

    def transformToLogMatrix_stream(self, actCol, unique=True, userData=None, productData=None):
        '''
        Low memory version of transformToLogMatrix
        Output a sparse matrix
        click products, unique == True consume 0.3% memory
        view products, unique == False consume 3% memory
        '''
        if userData == None:
            userData = self.userData
        if productData == None:
            productData = self.productData
        logs = self.resultData
        ID_to_row_dict = { x: i for i, x in enumerate(userData.id.unique()) }
        rowIndices = []
        colIndices = []
        datas = [] 
        for i, userID in enumerate(userData.id):
            actProducts = logs.loc[logs.id==userID, actCol].values[0]
            try:
                np.isnan(actProducts)
            except:
                if unique==True:
                    # care click or not, just like advertisement?
                    for productID in set(actProducts.split()):
                        rowIndices.append(ID_to_row_dict[userID])
                        colIndices.append(int(productID) - 1)
                        datas.append(1)
                elif unique==False:
                    actProductCount = Counter(actProducts.split())
                    for productID in actProductCount.keys():
                        rowIndices.append(ID_to_row_dict[userID])
                        colIndices.append(int(productID) - 1)
                        datas.append(actProductCount[productID])
                    del actProductCount
            del actProducts
            gc.collect()
        sparse_user_item_matrix = sparse.csr_matrix((datas, (rowIndices, colIndices)), shape=(len(userData.id.unique()), max(productData.id)))
        print "Stream construction of sparse log matrix complete "
        return sparse_user_item_matrix

    def transformAndSaveLogMatrix(self, actCol, unique=True, userData=None, productData=None):
        '''
        Low memory version of transformToLogMatrix_stream
        save user<--->product view logs to file to reduce RAM consumption
        But this method is extremely slow -> discarded!!
        fileName = './matrixes/user'+str(userRow)+'_view_products'+'_'+viewRowDate+'.npz'
        view_row = np.load(fileName)['userAct_row']
        ownViewProductSets = set(view_row.nonzero()[0])
        '''
        if userData == None:
            userData = self.userData
        if productData == None:
            productData = self.productData
        logs = self.resultData
        today = self.end_date.strftime('%m%d')
        ID_to_row_dict = { x: i for i, x in enumerate(userData.id.unique()) }
        for i, userID in enumerate(userData.id):
            oneRow = np.zeros(max(productData.id))
            actProducts = logs.loc[logs.id==userID, actCol].values[0]
            try:
                np.isnan(actProducts)
            except:
                if unique==True:
                    # care click or not, just like advertisement?
                    for productID in set(actProducts.split()):
                        oneRow[(int(productID)-1)] += 1
                elif unique==False:
                    for productID in actProducts.split():
                        oneRow[(int(productID)-1)] += 1
            fileName = './matrixes/user'+str(ID_to_row_dict[userID])+'_'+actCol+'_'+today
            np.savez(fileName, userAct_row=oneRow)
            del actProducts, oneRow
            gc.collect()
        print "Save and construct of user log row complete"

    def transformProductMatrix(self, productData=None, colWeights=[1,1,1], listCols=['_source.categories', '_source.colors', '_source.tags']):
        '''
        Transform product DF to product feature matrix!
        With these columns: 'categories', 'colors', 'tags'
        '''
        if productData==None:
            productData = self.productData
        colsToDrop = ['_index', '_score', '_source.available_stocks_count', '_source.can_order', '_source.can_send_arrival_notice',\
            '_source.should_charge_by_rental_price', '_source.open_for_order_at', '_source.open_for_order', 'sort', '_type', \
            '_source.created_at', '_source.name', '_source.product_photos_count', '_source.rental_price']
        # created time, source name jieba??? TODO
        df = productData.drop(colsToDrop, axis=1)
        stockCols = [ x for x in df.filter(regex='stocks').columns ]
        df = df.drop(stockCols, axis=1)
        #df['_source.categories'] = df['_source.categories'].apply(lambda x: x[0])
       
        #listCols = ['_source.categories', '_source.colors', '_source.seasons', '_source.shapes', '_source.tags', '_source.themes']
        newDrop = ['_source.seasons', '_source.shapes', '_source.themes']
        for j, col in enumerate(listCols):
            print 'Vectorizing ', col, ' with weight ', colWeights[j]
            pref_count = Counter()
            for val_list in df[col].values:
                pref_count.update(val_list)
            print pref_count
            newkeys = [ x for x in pref_count.keys() if x is not None ]
            pref_dict = { x: i for i,x in enumerate(newkeys) }
            matrix = np.zeros((len(df), len(pref_dict.keys())))
            for i, val_list in enumerate(df[col].values):
                for val in val_list:
                    if val is not None:
                        matrix[i, pref_dict[val]] = colWeights[j]
            sub_df = pd.DataFrame(matrix, columns=[col+'_'+x for x in sorted(pref_dict, key=pref_dict.get)])
            df = pd.concat([df, sub_df], axis=1)
            df = df.drop(col, axis=1)
        df = df.drop(newDrop, axis=1)

        # reindex to match dimension!
        newIndex = pd.Index(np.arange(max(df.id))+1)
        df = df.set_index("id").reindex(newIndex).reset_index()
        df = df.fillna(0)
        return df

    def _mergeLeft(self, left_df, right_df, left_id, right_id):
        mergeData = pd.merge(left_df, right_df, left_on=left_id, right_on=right_id, how='left')
        mergeData = mergeData.drop(right_id,1)
        return mergeData

    def _countProductTags(self, activity):
        dataframe = self.resultData
        tag_dict = self.tag_dict
        productData = self.productData
        productID_tag = {productData['id'].iloc[i]: productData['_source.tags'].iloc[i] for i in range(len(productData))}
        tag_counts = []
        valid_index = dataframe[activity+'_products'].notnull()
        for i, oneData in enumerate(dataframe[activity+'_products']):
            one_tag_counts = dict.fromkeys(sorted(tag_dict, key=tag_dict.get), 0)
            if valid_index.iloc[i]:
                for productID in dataframe[activity+'_products'].iloc[i].split():
                    try:
                        np.isnan(productID_tag[int(productID)])
                    except:
                        try:
                            for productTag in productID_tag[int(productID)]:
                                one_tag_counts[productTag] += 1
                        except:
                            print 'Product id: ', productID, ' is deleted!'
            tag_counts.append(one_tag_counts)
        tag_counts_df = pd.DataFrame(tag_counts)
        tag_counts_df.columns = tag_counts_df.columns+'_'+activity
        return tag_counts_df

    def _loadUserData(self, end_date=None):
        '''
        Load user data of 'end_date'
        Read user json from DATA_DIRECTORY of config.py
        For 16GB ram => cost 5% memory => approx. 820 MB
        Take approx. 30 seconds?
        '''
        if end_date == None:
            end_date = self.end_date
        today = end_date.strftime("%m%d")
        filename = os.path.join(self.cfg.DATA_DIRECTORY, 'users_'+today+'.json')
        userJson = pd.read_json(filename)
        flattenDF = pd.io.json.json_normalize(userJson['hits']['hits'])
        del userJson
        colToUse = ['_id', '_source.general_type']
        prefCols = [col for col in flattenDF.columns if 'preference' in col]
        userData = flattenDF[colToUse+prefCols]
        userData = userData.rename(columns = {'_id':'id'})
        userData.id = userData.id.astype(int)
        userData.drop_duplicates('id', inplace=True)
        userData.sort_values(by='id', inplace=True)
        userData = userData.reset_index().drop('index', axis=1)
        del flattenDF
        self.userData = userData

    def _loadUserData_stream(self, end_date=None):
        '''
        Load user data of 'end_date'
        Try to use less memories
        Use approx. 1.5% of 16G ram => 245 MB
        Read user json from DATA_DIRECTORY of config.py
        '''
        if end_date == None:
            end_date = self.end_date
        today = end_date.strftime("%m%d")
        filename = os.path.join(self.cfg.DATA_DIRECTORY, 'users_'+today+'.json')
        users = []
        for value in ijson.items(open(filename, 'rb'), 'hits.hits.item'):
            one_user = {}
            one_user['id'] = value['_id']
            one_user['_source.general_type'] = value['_source']['general_type']
            for pref in value['_source']['preferences'].keys():
                one_user['_source.preferences.'+pref] = value['_source']['preferences'][pref]
            users.append(one_user)
        # tranform to DF?
        userData = pd.DataFrame(users)
        del users
        userData.id = userData.id.astype(int)
        userData.drop_duplicates('id', inplace=True)
        userData.sort_values(by='id', inplace=True)
        userData = userData.reset_index().drop('index', axis=1)
        self.userData = userData
        gc.collect()

    def _loadProductData(self, end_date=None):
        '''
        Load product data of 'end_date'
        Read product json from DATA_DIRECTORY of config.py
        Consume 0.3% memory
        '''
        if end_date == None:
            end_date = self.end_date
        today = end_date.strftime("%m%d")
        filename = os.path.join(self.cfg.DATA_DIRECTORY, 'products_'+today+'.json')
        productJson = pd.read_json(filename)
        flattenDF = pd.io.json.json_normalize(productJson['hits']['hits'])
        del productJson
        #productData = flattenDF[['_id', '_source.tags']]
        productData = flattenDF.rename(columns = {'_id':'id'})
        productData.id = productData.id.astype(int)
        productData.drop_duplicates('id', inplace=True)
        productData.sort_values(by='id', inplace=True)
        productData = productData.reset_index().drop('index', axis=1)
        del flattenDF
        self.productData = productData

    def _loadProductData_stream(self, end_date=None):
        '''
        Consume 0.2% memory
        '''
        if end_date == None:
            end_date = self.end_date
        today = end_date.strftime("%m%d")
        filename = os.path.join(self.cfg.DATA_DIRECTORY, 'products_'+today+'.json')
        products = []
        for value in ijson.items(open(filename, 'rb'), 'hits.hits.item'):
            one_product = {}
            one_product['id'] = value['_id']
            one_product['_index'] = value['_index']
            one_product['_score'] = value['_score']
            one_product['_type'] = value['_type']
            one_product['sort'] = value['sort']
            for pref in value['_source'].keys():
                one_product['_source.'+pref] = value['_source'][pref]
            products.append(one_product)
        # tranform to DF?
        productData = pd.DataFrame(products)
        del products
        productData.id = productData.id.astype(int)
        productData.drop_duplicates('id', inplace=True)
        productData.sort_values(by='id', inplace=True)
        productData = productData.reset_index().drop('index', axis=1)
        self.productData = productData
        gc.collect()

    def _loadTag(self, tagFile='./raw_data/tags_match_0710.csv'):
        '''Load tag match file'''
        tags_match = pd.read_csv(tagFile, encoding='utf-8')
        tags_match.sort_values('name', inplace=True)
        # vectorize tags  ## get sorted keys: sorted(tag_dict, key=tag_dict.get)
        chinese_tags = tags_match.name
        # tag_dict_rev = {i: chinese_tags.iloc[i] for i in range(len(chinese_tags)) }
        self.tag_dict = {chinese_tags.iloc[i]: i for i in range(len(chinese_tags)) }
        