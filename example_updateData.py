'''
This part shows how to update resultData,
rather than run through all of the preprocessing
'''
from datetime import date, timedelta
from scipy import sparse

import config as cfg
from DataLoader import DataLoader

 
start_date = date(2017,3,23)
end_date = (date.today() - timedelta(1))
resultInFile = './mergeData_0716.csv'

DL = DataLoader(cfg ,start_date, date(2017,7,16))
newDay = date(2017,7,17)
# below only update newDay's record to existing resultData
DL.loadData(end_date = newDay)
DL.loadResultData(resultInFile)
DL.updateData(newDay)

resultOutFile = './example_resultData.csv'
DL.saveData(dataName='resultData', fileName=resultOutFile)

'''
If update through startDay to newDay (ex. 07/17~07/25)
  Make sure the latest user and product data is downloaded (run dailywork.sh)
  - DL.loadData(end_date = newDay)
  - DL.loadResultData(resultFile)
  - DL.updateData(startDay)
  - DL.updateData(startDay+1)
  - ... until DL.updataData(newDay)

Remember to save updated resultData when finish!!
  - resultOutFile = './example_resultData.csv'
  - DL.saveData(dataName='resultData', fileName=resultOutFile)
'''
