import pandas as pd

#RAW DATASETS
customersdf = pd.read_csv('_data/customers.csv')
devicesDf = pd.read_csv('_data/device.csv')
towersdf = pd.read_csv('_data/towers.csv')
callsdf = pd.read_csv('_data/calls.csv')

# secondary raw datasets for reuse 
devicesDfb = pd.read_csv('_data/device.csv')
customersdfb = pd.read_csv('_data/customers.csv')

#functions to clean datasets 
def cleanCustomersDf(customersdf):
        #clean Dataframe  
        customersdf = customersdf
        customersdf=customersdf.drop(['Unnamed: 0'],axis=1)
        customersdf.dropna(subset=["id_number"], axis=0, inplace=True)
        customersdf['province'] =customersdf['province'].str.lower()
        customersdf['customer_type'] =customersdf['customer_type'].str.lower()
        customersdf.dropna(subset=["province"], axis=0, inplace=True)
        customersdf=customersdf.sort_values('province',key=lambda x:x.str.len()) #check provinces entered correctly
        customersdf['id_number'] =customersdf['id_number'].abs()
        
        return customersdf      
def cleanDevicesDf(devicesDf):
    #clean Dataframe 

    devicesDf =devicesDf
    devicesDf['owner']=devicesDf['owner'].abs()
    devicesDf['phone_number']=devicesDf['phone_number'].str[-9:]
    devicesDf.rename(columns = {'owner':'id_number'}, inplace = True)
    devicesDf=devicesDf.drop(['Unnamed: 0'],axis=1)
    devicesDf.tail()

    devicesDf.drop_duplicates(subset ="phone_number",keep = False, inplace = True)
    devicesDf.head()
    return devicesDf  
def cleanTowersDF(towersdf):
        #CLEAN towers DATAFRAME  
        towersdf = towersdf
        towersdf=towersdf.drop(['Unnamed: 0'],axis=1) #drop unwanted column
            #columns to lowercase 
        towersdf['province']=towersdf['province'].str.lower() 
        towersdf['tower_type']=towersdf['tower_type'].str.lower()

            #correct incorrect province entries
        towersdf['province'].replace("west", "western", inplace = True)
        towersdf['province'].replace("north", "northern", inplace = True)
        towersdf['province'].replace("south", "southern", inplace = True)
        towersdf['province'].replace("east", "eastern", inplace = True)
        towersdf['province'].replace(" ", "", inplace = True)
            
            #clean Tower ID and Strip off unwanted characters 
        towersCol = pd.DataFrame(towersdf['tower_id'].str.split(':|#', expand=True))
        towersdf.insert(len(towersdf.columns), 'towerID', towersCol[1]) 
        towersdf=towersdf.drop(['tower_id'],axis=1)
        towersdf.dropna(subset=["towerID"], axis=0, inplace=True)
            #drop duplicates if any
        towersdf.drop_duplicates(subset ="towerID",keep = False, inplace = True)
        
        return towersdf
def cleanCallsDf (callsdf):
    
    callsdf=callsdf
    callsdf['origin_number'] = callsdf['origin_number'].str[-9:]
    callsdf['receive_number'] = callsdf['receive_number'].str[-9:]

    originCol = callsdf['origin_tower'].str.split(':|#', expand=True)

    OldList = []
    NewList = [] 

    for number in originCol[1]:
        NewList.append(number)
    for number in originCol[0]:
        OldList.append(number)
        
    for i in range(len(NewList)):
        if NewList[i] == None:
            NewList[i] = OldList[i]
            
    NewList = [x.strip(' ') for x in NewList]      
    len(NewList)

    receiveCol = callsdf['receive_tower'].str.split(':|#', expand=True)

    OldreceiveList = []
    NewreceiveList = [] 

    for number in receiveCol[1]:
        NewreceiveList.append(number)
    for number in receiveCol[0]:
        OldreceiveList.append(number)
        
    for i in range(len(NewreceiveList)):
        if NewreceiveList[i] == None:
            NewreceiveList[i] = OldreceiveList[i]
            
    len(NewreceiveList)

    NewreceiveList = [x.strip(' ') for x in NewreceiveList]      

    callsdf.insert(len(towersdf.columns), 'originTower', NewList)         
    callsdf.insert(len(towersdf.columns), 'receiveTower', NewreceiveList) 

    callsdf=callsdf.drop(['origin_tower','receive_tower','Unnamed: 0'],axis=1)
    
    return callsdf

#Cleaned Dataframes 
customersdf= cleanCustomersDf(customersdf)
devicesDf=cleanDevicesDf(devicesDf)
towersdf= cleanTowersDF(towersdf)
callsdf= cleanCallsDf(callsdf)

customersdb= cleanCustomersDf(customersdfb)
devicesDfb=cleanDevicesDf(devicesDfb)

def num_customers_per_customer_region(customersdf):
    """
    Gets the number of customers per customer region given the raw dataset. 
    Note - code will have to be written to clean and process the dataset.
    Args:
        data (pandas.DataFrame): The raw dataset 
    Returns:
        pandas.DataFrame: result
    """
    # START CODE HERE
    # END CODE HERE
    customersdf = customersdf
    Regionsdf=customersdf['province'].value_counts(ascending=False)
    
    #No. of Customers per customer region
    Regionsdf=pd.DataFrame(Regionsdf)
    Regionsdf=Regionsdf.reset_index(drop=False)
    Regionsdf.rename(columns = {'index':'Region','province':'No. of Customers'}, inplace = True)
    print("No. Customers per region\n",Regionsdf,"\n\n")
    return Regionsdf
def num_devices_per_customer_region(devicesDf):
    """
    Gets the number of devices per customer region given the raw dataset. 
    Note - code will have to be written to clean and process the dataset.

    Args:
        data (pandas.DataFrame): The raw dataset 
    
    Returns:
        pandas.DataFrame: result
    """
    # START CODE HERE

    # create a dataframe with owners and number of devices 
    ownersdevicesDf=pd.DataFrame(devicesDf['id_number'].value_counts(ascending=False))
    ownersdevicesDf=ownersdevicesDf.reset_index(drop=False)
    ownersdevicesDf.rename(columns = {'index':'id_number','id_number':'No. of Devices'}, inplace = True)

    
    #JOIN TABLES MERGE Devices per customer region  , CUSTOMERS WITH DEVICES
    devicesRegion = pd.merge(ownersdevicesDf, customersdf,on="id_number")
    devicesRegion.head().style.set_caption("No. of Devices per customer")

    #GROUP BY REGION AND GET SUM OF DEVICES
    devicesperRegion = devicesRegion.groupby('province')['No. of Devices'].sum().sort_values(ascending=False)
    devicesperRegion = pd.DataFrame(devicesperRegion)
    devicesperRegion = devicesperRegion.reset_index(drop=False)
    devicesperRegion
    devicesperRegion.style.set_caption("No. Devices Per Customer Region")
    
    print("No. devices per Customer Region\n",devicesperRegion,"\n\n")
    # END CODE HERE
    return devicesperRegion
def num_towers_per_customer_region(towersdf):
    """
    Gets the number of towers per customer region given the raw dataset. 
    Note - code will have to be written to clean and process the dataset.

    Args:
        data (pandas.DataFrame): The raw dataset 
    
    Returns:
        pandas.DataFrame: result
    """
    # START CODE HERE

  
    #GROUP towers by region count 
    towersRegion = towersdf.groupby('province')['towerID'].count()
    towersRegion = pd.DataFrame(towersRegion)
    towersRegion=towersRegion.reset_index(drop=False)
    towersRegion.rename(columns = {'towerID':'No. of Towers'}, inplace = True)

    # END CODE HERE
    print("No. towers per Customer Region\n",towersRegion,"\n\n")
    return towersRegion
def num_calls_received_per_customer_region(callsdf):
    """
    Gets the number of calls received per customer region given the raw dataset. 
    Note - code will have to be written to clean and process the dataset.
    Args:
        data (pandas.DataFrame): The raw dataset 
    Returns:
        pandas.DataFrame: result
    """

    #create a dataframe for received calls 
    receiveTowersDf= pd.DataFrame(callsdf[['receiveTower','receive_number']])
    receiveTowersDf.rename(columns = {'receiveTower':'towerID'}, inplace = True)

    #Received tower region 
    #receiveTowersDf
    receivedRegion = pd.merge(towersdf, receiveTowersDf,how="right",on="towerID")
    receivedRegion['province'].replace("south", "southern", inplace = True)
    receivedRegion.dropna(subset=["province"], axis=0, inplace=True)

    #GROUP received calls on towers by region  
    newreceivedRegion = receivedRegion.groupby('province')['towerID'].count()
    newreceivedRegion = pd.DataFrame(newreceivedRegion)
    newreceivedRegion = newreceivedRegion.reset_index(drop=False)
    newreceivedRegion.rename(columns = {'towerID':'No. of Calls'}, inplace = True)
    newreceivedRegion
    
    print("No. Calls received per Customer Region\n",newreceivedRegion,"\n\n")
    # END CODE HERE
    return newreceivedRegion
def num_calls_made_per_customer_region(callsdf,towersdf):
    """
    Gets the number of calls made per customer region given the raw dataset. 
    Note - code will have to be written to clean and process the dataset.

    Args:
        data (pandas.DataFrame): The raw dataset 
    
    Returns:
        pandas.DataFrame: result
    """
    # START CODE HERE
 
    #clean tower columns
    #Create new DF with ORIGIN and DURATION 
    originTowerDurationDF = pd.DataFrame(callsdf[['originTower','origin_number','duration']])
    originTowerDurationDF.rename(columns = {'originTower':'towerID'}, inplace = True)

    originRegion = pd.merge(towersdf, originTowerDurationDF,how="right",on="towerID")
    originRegion.dropna(subset=["province"], axis=0, inplace=True)

    neworiginRegion = originRegion.groupby('province')['towerID'].count()
    neworiginRegion = pd.DataFrame(neworiginRegion)
    neworiginRegion = neworiginRegion.reset_index(drop=False)
    neworiginRegion.rename(columns = {'towerID':'No. of Calls'}, inplace = True)
    
    print("No. Calls made per Customer Region\n",neworiginRegion,"\n\n")

    # END CODE HERE
    return neworiginRegion
def revenue_per_customer_region(devicesDf,customersdf):
    """
    Gets the revenue per customer region given the raw dataset. 
    Note - code will have to be written to clean and process the dataset.

    Args:
        data (pandas.DataFrame): The raw dataset 
    
    Returns:
        pandas.DataFrame: result
    """
        # START CODE HERE
    ownersdevicesDf=pd.DataFrame(devicesDf['id_number'].value_counts(ascending=False))
    ownersdevicesDf=ownersdevicesDf.reset_index(drop=False)
    ownersdevicesDf.rename(columns = {'index':'id_number','id_number':'No. of Devices'}, inplace = True)
    
    devicesRegion = pd.merge(ownersdevicesDf, customersdf,on="id_number")
    devicesRegion.head().style.set_caption("No. of Devices per customer")

    devicesDFtemp = devicesDf
    devicesDFtemp.rename(columns = {'phone_number':'origin_number'}, inplace = True)
    devicesDFtemp.head()
    devicesDFtemp['id_number']=devicesDFtemp['id_number'].astype('object')

    originTowerDurationDF = pd.DataFrame(callsdf[['originTower','origin_number','duration']])
    originTowerDurationDF.rename(columns = {'originTower':'towerID'}, inplace = True)

    originRegion = pd.merge(towersdf, originTowerDurationDF,how="right",on="towerID")
    originRegion.dropna(subset=["province"], axis=0, inplace=True)

    # CREATE NEW DF modified calls with ownwer ID
    originRegionRevenue = pd.merge(devicesDFtemp, originRegion, how="right",on="origin_number")
    originRegionRevenue['id_number']=originRegionRevenue['id_number'].astype('object')
    originRegionRevenue.dropna(subset=["id_number"], axis=0, inplace=True)

    customersdftemp = customersdf[['id_number','customer_type']]

    customersdftemp['id_number']=customersdftemp['id_number'].astype('object')

    newRevenueDF = pd.merge(customersdftemp, originRegionRevenue, how="right",on="id_number")

        #CALC REVENUE MERGE CALLS DF
    newRevenueDFb = pd.merge(callsdf[['origin_number','receive_number','receiveTower']], newRevenueDF, how="right",on="origin_number")
    newRevenueDFb.dropna(subset=["province"], axis=0, inplace=True)
    newRevenueDFb.rename(columns = {'province':'ogprovince','id_number':'ogid_number','customer_type':'ogcustomer_type'}, inplace = True)

    devicesDFtempb = devicesDFtemp
    devicesDFtempb.rename(columns = {'origin_number':'receive_number','id_number':'rcid_number'}, inplace = True)
    devicesDFtempb.head()

    newRevenueDFc = pd.merge(newRevenueDFb, devicesDFtempb, how="left",on="receive_number")
    newRevenueDFc.dropna(subset=["rcid_number"], axis=0, inplace=True)
    newRevenueDFc.tail()

    customersdftemp.rename(columns = {'id_number':'rcid_number','customer_type':'rccustomer_type'}, inplace = True)

    #CREATE A DATAFRAME WITH CUSTOMER TYPES and durations 
    newRevenueDFd = pd.merge(newRevenueDFc, customersdftemp, how="left",on="rcid_number")
    newRevenueDFd.dropna(subset=["rccustomer_type","ogcustomer_type","rccustomer_type"], axis=0, inplace=True)
    newRevenueDFd = newRevenueDFd.reset_index(drop=True)

    #CALCULATE CALL COSTS AND CREATE REVENUE COLUMN  START HERE 
    #ADD BASE CHARGE create a lst
    baseCharges=[] 

    #clean tower column
    originCustomerType = newRevenueDFd['ogcustomer_type'] #

    callType = []

    for call in newRevenueDFd['ogcustomer_type']:
        #callType.append(call)
            call = call 
            if call == 'normal':
                basecharge=200
                baseCharges.append(basecharge)
            elif call== 'vip':
                basecharge=500
                baseCharges.append(basecharge)
            elif call == 'basic':
                basecharge=0
                baseCharges.append(basecharge)

    #print(baseCharges)
    baseChargesColumn=pd.DataFrame(baseCharges)
    print(baseChargesColumn.shape)
    baseChargesColumn.rename(columns= {0:'baseCharge'}, inplace = True)

    #add base charges to Dataframe 
    newRevenueDFd['basecharge']=baseChargesColumn['baseCharge']


    #add call cost column 
    #weght columns and use numbers 

    callTypesog=[] 

    callTypesrc = []

    callCost=[]

    calltypenum=[]

    length = len(callTypesog)

    for call in newRevenueDFd['ogcustomer_type']:
        #callType.append(call)
              callTypesog.append(call)

    for call in newRevenueDFd['rccustomer_type']:
        #callType.append(call)
            callTypesrc.append(call)

    for i in range(len(callTypesog)):
        if callTypesog[i] =="vip":
            receiver=callTypesrc[i]
            if receiver == "basic":
                cost= 0.05
                callCost.append(cost)
            elif receiver== "normal":
                cost= 0.05
                callCost.append(cost)
            elif receiver == "vip":
                cost= 0
                callCost.append(cost)
        elif callTypesog[i] == "normal":
            receiver=callTypesrc[i]
            if receiver == "basic":
                cost= 0.15
                callCost.append(cost)
            elif receiver == "normal":
                cost= 0.15
                callCost.append(cost)
            elif receiver == "vip":
                cost= 0.15
                callCost.append(cost)

        elif callTypesog[i] == "basic":
            receiver=callTypesrc[i]
            if receiver == "basic":
                cost= 0.30
                callCost.append(cost)
            elif receiver=="normal":
                cost= 0.30
                callCost.append(cost)
            elif receiver == "vip":
                cost= 0.30
                callCost.append(cost)

    callCostDF=pd.DataFrame(callCost)
    print(callCostDF.shape)
    callCostDF.rename(columns= {0:'cost'}, inplace = True)

    # add cost to dataframe and 
    newRevenueDFd['cost']= callCostDF['cost']
    newRevenueDFd['callcharge']= (newRevenueDFd['cost']*newRevenueDFd['duration'])+ newRevenueDFd['basecharge']

    customersdftempb= customersdf
    customersdftempb.rename(columns = {'id_number':'rcid_number','province':'rc_province'}, inplace = True)

    newRevenueDFd = pd.merge(newRevenueDFd, customersdftempb,on="rcid_number")


    # OVERVIEW Revenue per customer region 
    revenueperRegion = newRevenueDFd.groupby('ogprovince')['callcharge'].sum() 
    revenueperRegion = pd.DataFrame(revenueperRegion)
    revenueperRegion = revenueperRegion.reset_index(drop=False)
    revenueperRegion.rename(columns = {'ogprovince':'Province','callcharge':'revenue (ZAR)'}, inplace = True)

    print("Total Revenue Per Customer Region\n",revenueperRegion,"\n\n")
    # END CODE HERE
    return revenueperRegion
def num_devices_per_customer(customersdfb,devicesDfb):
    """
    Gets the number of devices per customer given the raw dataset. 
    Note - code will have to be written to clean and process the dataset.
    Args:
        data (pandas.DataFrame): The raw dataset 
    Returns:
        pandas.DataFrame: result
    """
    # START CODE HERE
    ownersdevicesDf=pd.DataFrame(devicesDfb['id_number'].value_counts(ascending=False))
    ownersdevicesDf=ownersdevicesDf.reset_index(drop=False)
    ownersdevicesDf.rename(columns = {'index':'id_number','id_number':'No. of Devices'}, inplace = True)
    
    devicesRegion = pd.merge(ownersdevicesDf, customersdfb,on="id_number")
    devicesRegion.head().style.set_caption("No. of Devices per customer")

    print("No. devices per Customer\n",devicesRegion,"\n\n")
    # END CODE HERE
    return devicesRegion
def num_calls_per_customer(devicesDfb,customersdfb,towersdf):
    """
    Gets the number of calls per customer given the raw dataset. 
    Note - code will have to be written to clean and process the dataset.
    Args:
        data (pandas.DataFrame): The raw dataset 
    Returns:
        pandas.DataFrame: result
    """
    
    # START CODE HERE

 
    devicesDFtemp = devicesDfb
    devicesDFtemp.rename(columns = {'phone_number':'origin_number'}, inplace = True)
    
    devicesDFtemp['id_number']=devicesDFtemp['id_number'].astype('object')

    originTowerDurationDF = pd.DataFrame(callsdf[['originTower','origin_number','duration']])
    originTowerDurationDF.rename(columns = {'originTower':'towerID'}, inplace = True)

    originRegion = pd.merge(towersdf, originTowerDurationDF,how="right",on="towerID")
    originRegion.dropna(subset=["province"], axis=0, inplace=True)

    # CREATE NEW DF modified calls with ownwer ID
    originRegionRevenue = pd.merge(devicesDFtemp, originRegion, how="right",on="origin_number")
    originRegionRevenue['id_number']=originRegionRevenue['id_number'].astype('object')
    originRegionRevenue.dropna(subset=["id_number"], axis=0, inplace=True)
 
    #create a temporary subset of customer df dataframe
    customersdftemp = customersdfb[['id_number','customer_type']]
    customersdftemp['id_number']=customersdftemp['id_number'].astype('object')

    # CREATE NEW DF modified calls with ownwer ID
    newRevenueDF = pd.merge(customersdftemp, originRegionRevenue, how="right",on="id_number")

    #CALC REVENUE MERGE CALLS DF
    newRevenueDFb = pd.merge(callsdf[['origin_number','receive_number','receiveTower']], newRevenueDF, how="right",on="origin_number")
    newRevenueDFb.dropna(subset=["province"], axis=0, inplace=True)
    newRevenueDFb.rename(columns = {'province':'ogprovince','id_number':'ogid_number','customer_type':'ogcustomer_type'}, inplace = True)

    devicesDFtempb = devicesDFtemp
    devicesDFtempb.rename(columns = {'origin_number':'receive_number','id_number':'rcid_number'}, inplace = True)

    
    newRevenueDFc = pd.merge(newRevenueDFb, devicesDFtempb, how="left",on="receive_number")
    newRevenueDFc.dropna(subset=["rcid_number"], axis=0, inplace=True)
    newRevenueDFc.tail()

    customersdftemp.rename(columns = {'id_number':'rcid_number','customer_type':'rccustomer_type'}, inplace = True)

    #CREATE A DATAFRAME WITH CUSTOMER TYPES and durations 
    newRevenueDFd = pd.merge(newRevenueDFc, customersdftemp, how="left",on="rcid_number")
    newRevenueDFd.dropna(subset=["rccustomer_type","ogcustomer_type","rccustomer_type"], axis=0, inplace=True)
    newRevenueDFd = newRevenueDFd.reset_index(drop=True)
    
    #-	No. of Calls per customer (from/to) DONE 
    callsperCustomerDF= newRevenueDFd.groupby('ogid_number')['ogid_number'].count().sort_values(ascending=False)
    callsperCustomerDF = pd.DataFrame(callsperCustomerDF)
    callsperCustomerDF.rename(columns = {'ogid_number':'No. Calls'}, inplace = True)
    callsperCustomerDF = callsperCustomerDF.reset_index(drop=False)
    callsperCustomerDF.rename(columns = {'ogid_number':'id_number'}, inplace = True)
    callsperCustomerDF['id_number'] = callsperCustomerDF['id_number'].astype('int')
    callsperCustomerDF.head().style.set_caption("No. of Calls per customer (from)")

    print("No. Calls per Customer \n",callsperCustomerDF,"\n\n")
    # END CODE HERE
    return callsperCustomerDF
def num_regions_called_per_customer(devicesDfb,customersdfb,towersdf,callsdf):
    """
    Gets the number of regions per customer called given the raw dataset. 
    Note - code will have to be written to clean and process the dataset.
    Args:
        data (pandas.DataFrame): The raw dataset 
    Returns:
        pandas.DataFrame: result
    """
    # START CODE HERE
    # assuming this raw data is the device.csv dataset  
   
    devicesDFtemp = devicesDfb
    devicesDFtemp.rename(columns = {'receive_number':'origin_number','rcid_number':'id_number'}, inplace = True)
    devicesDFtemp['id_number']=devicesDFtemp['id_number'].abs()
    devicesDFtemp['id_number']=devicesDFtemp['id_number'].astype('object')

    originTowerDurationDF = pd.DataFrame(callsdf[['originTower','origin_number','duration']])
    originTowerDurationDF.rename(columns = {'originTower':'towerID'}, inplace = True)

    originRegion = pd.merge(towersdf, originTowerDurationDF,how="right",on="towerID")
    originRegion.dropna(subset=["province"], axis=0, inplace=True)  

    # CREATE NEW DF modified calls with ownwer ID
    devicesDFtemp.rename(columns = {'phone_number':'origin_number'}, inplace = True)
    originRegionRevenue = pd.merge(devicesDFtemp, originRegion, how="right",on="origin_number")
    originRegionRevenue['id_number']=originRegionRevenue['id_number'].astype('object')
    originRegionRevenue.dropna(subset=["id_number"], axis=0, inplace=True)
    originRegionRevenue.head()
    

    #create a temporary subset of customer df dataframe
    customersdftemp = customersdfb[['id_number','customer_type']]
    customersdftemp['id_number']=customersdftemp['id_number'].abs()
    customersdftemp['id_number']=customersdftemp['id_number'].astype('object')

    # CREATE NEW DF modified calls with ownwer ID
    newRevenueDF = pd.merge(customersdftemp, originRegionRevenue, how="right",on="id_number")

    #CALC REVENUE MERGE CALLS DF
    newRevenueDFb = pd.merge(callsdf[['origin_number','receive_number','receiveTower']], newRevenueDF, how="right",on="origin_number")
    newRevenueDFb.dropna(subset=["province"], axis=0, inplace=True)
    newRevenueDFb.rename(columns = {'province':'ogprovince','id_number':'ogid_number','customer_type':'ogcustomer_type'}, inplace = True)

    devicesDFtempb = devicesDFtemp
    devicesDFtempb.rename(columns = {'origin_number':'receive_number','id_number':'rcid_number'}, inplace = True)


    newRevenueDFc = pd.merge(newRevenueDFb, devicesDFtempb, how="left",on="receive_number")
    newRevenueDFc.dropna(subset=["rcid_number"], axis=0, inplace=True)
    newRevenueDFc.tail()

    customersdftemp.rename(columns = {'id_number':'rcid_number','customer_type':'rccustomer_type'}, inplace = True)

    #CREATE A DATAFRAME WITH CUSTOMER TYPES and durations 
    newRevenueDFd = pd.merge(newRevenueDFc, customersdftemp, how="left",on="rcid_number")
    newRevenueDFd.dropna(subset=["rccustomer_type","ogcustomer_type","rccustomer_type"], axis=0, inplace=True)
    newRevenueDFd = newRevenueDFd.reset_index(drop=True)
    
    regionscalledPC= newRevenueDFd.groupby('ogid_number')['ogprovince'].count().sort_values(ascending=False)
    regionscalledPC = pd.DataFrame(regionscalledPC)
    # regionscalledPC.rename(columns = {'ogid_number':'No. Calls'}, inplace = True)
    regionscalledPC = regionscalledPC.reset_index(drop=False)
    regionscalledPC.rename(columns = {'ogprovince':'Regions Called','ogid_number':'id_number'}, inplace = True)
    regionscalledPC['id_number'] = regionscalledPC['id_number'].astype('int')
    regionscalledPC.head().style.set_caption("No. Regions called per customer")

    print("No Regions calld per Customer\n",regionscalledPC,"\n")    # END CODE HERE
    return regionscalledPC
def num_towers_called_per_customer(devicesDfb,customersdfb,callsdf,towersdf):
    """
    Gets the number of towers called per customer given the raw dataset. 
    Note - code will have to be written to clean and process the dataset.

    Args:
        data (pandas.DataFrame): The raw dataset 
    
    Returns:
        pandas.DataFrame: result
    """
    # START CODE HERE
  
    devicesDFtemp = devicesDfb
    devicesDFtemp.rename(columns = {'receive_number':'origin_number','rcid_number':'id_number'}, inplace = True)
    devicesDFtemp['id_number']=devicesDFtemp['id_number'].abs()
    devicesDFtemp['id_number']=devicesDFtemp['id_number'].astype('object')

    originTowerDurationDF = pd.DataFrame(callsdf[['originTower','origin_number','duration']])
    originTowerDurationDF.rename(columns = {'originTower':'towerID'}, inplace = True)

    originRegion = pd.merge(towersdf, originTowerDurationDF,how="right",on="towerID")
    originRegion.dropna(subset=["province"], axis=0, inplace=True)  

    # CREATE NEW DF modified calls with ownwer ID
    devicesDFtemp.rename(columns = {'phone_number':'origin_number'}, inplace = True)
    originRegionRevenue = pd.merge(devicesDFtemp, originRegion, how="right",on="origin_number")
    originRegionRevenue['id_number']=originRegionRevenue['id_number'].astype('object')
    originRegionRevenue.dropna(subset=["id_number"], axis=0, inplace=True)
    originRegionRevenue.head()
    

    #create a temporary subset of customer df dataframe
    customersdftemp = customersdfb[['id_number','customer_type']]
    customersdftemp['id_number']=customersdftemp['id_number'].abs()
    customersdftemp['id_number']=customersdftemp['id_number'].astype('object')

    # CREATE NEW DF modified calls with ownwer ID
    newRevenueDF = pd.merge(customersdftemp, originRegionRevenue, how="right",on="id_number")

    #CALC REVENUE MERGE CALLS DF
    newRevenueDFb = pd.merge(callsdf[['origin_number','receive_number','receiveTower']], newRevenueDF, how="right",on="origin_number")
    newRevenueDFb.dropna(subset=["province"], axis=0, inplace=True)
    newRevenueDFb.rename(columns = {'province':'ogprovince','id_number':'ogid_number','customer_type':'ogcustomer_type'}, inplace = True)

    devicesDFtempb = devicesDFtemp
    devicesDFtempb.rename(columns = {'origin_number':'receive_number','id_number':'rcid_number'}, inplace = True)


    newRevenueDFc = pd.merge(newRevenueDFb, devicesDFtempb, how="left",on="receive_number")
    newRevenueDFc.dropna(subset=["rcid_number"], axis=0, inplace=True)
    newRevenueDFc.tail()

    customersdftemp.rename(columns = {'id_number':'rcid_number','customer_type':'rccustomer_type'}, inplace = True)

    #CREATE A DATAFRAME WITH CUSTOMER TYPES and durations 
    newRevenueDFd = pd.merge(newRevenueDFc, customersdftemp, how="left",on="rcid_number")
    newRevenueDFd.dropna(subset=["rccustomer_type","ogcustomer_type","rccustomer_type"], axis=0, inplace=True)
    newRevenueDFd = newRevenueDFd.reset_index(drop=True)
    
    newRevenueDFx = newRevenueDFd
    #	No. of Towers called per customer (from/to)
    TowersFromPC= newRevenueDFd.groupby('ogid_number')['towerID'].count().sort_values(ascending=False)
    TowersFromPC = pd.DataFrame(TowersFromPC)
    #regionscalledPC.rename(columns = {'ogid_number':'No. Calls'}, inplace = True)
    TowersFromPC = TowersFromPC.reset_index(drop=False)
    TowersFromPC.rename(columns = {'ogid_number':'id_number','towerD':'No of Towers'}, inplace = True)
    TowersFromPC['id_number'] = TowersFromPC['id_number'].astype('int')
    TowersFromPC.head().style.set_caption("No. Towers called per customer")

    print("No. Towers Called per Customer\n",TowersFromPC,"\n\n")
    # END CODE HERE
    return TowersFromPC
def total_revenue_per_customer(devicesDfb,customersdfb,callsdf,towersdf):
    """
    Gets the total revenue per customer given the raw dataset. 
    Note - code will have to be written to clean and process the dataset.
    Args:
        data (pandas.DataFrame): The raw dataset 
    Returns:
        pandas.DataFrame: result
    """
    res = None
    # START CODE HERE

    devicesDFtemp = devicesDfb
    devicesDFtemp.rename(columns = {'receive_number':'origin_number','rcid_number':'id_number'}, inplace = True)
    devicesDFtemp['id_number']=devicesDFtemp['id_number'].abs()
    devicesDFtemp['id_number']=devicesDFtemp['id_number'].astype('object')

    originTowerDurationDF = pd.DataFrame(callsdf[['originTower','origin_number','duration']])
    originTowerDurationDF.rename(columns = {'originTower':'towerID'}, inplace = True)

    originRegion = pd.merge(towersdf, originTowerDurationDF,how="right",on="towerID")
    originRegion.dropna(subset=["province"], axis=0, inplace=True)  

    # CREATE NEW DF modified calls with ownwer ID
    devicesDFtemp.rename(columns = {'phone_number':'origin_number'}, inplace = True)
    originRegionRevenue = pd.merge(devicesDFtemp, originRegion, how="right",on="origin_number")
    originRegionRevenue['id_number']=originRegionRevenue['id_number'].astype('object')
    originRegionRevenue.dropna(subset=["id_number"], axis=0, inplace=True)
    originRegionRevenue.head()
    

    #create a temporary subset of customer df dataframe
    customersdftemp = customersdfb[['id_number','customer_type']]
    customersdftemp['id_number']=customersdftemp['id_number'].abs()
    customersdftemp['id_number']=customersdftemp['id_number'].astype('object')

    # CREATE NEW DF modified calls with ownwer ID
    newRevenueDF = pd.merge(customersdftemp, originRegionRevenue, how="right",on="id_number")

    #CALC REVENUE MERGE CALLS DF
    newRevenueDFb = pd.merge(callsdf[['origin_number','receive_number','receiveTower']], newRevenueDF, how="right",on="origin_number")
    newRevenueDFb.dropna(subset=["province"], axis=0, inplace=True)
    newRevenueDFb.rename(columns = {'province':'ogprovince','id_number':'ogid_number','customer_type':'ogcustomer_type'}, inplace = True)

    devicesDFtempb = devicesDFtemp
    devicesDFtempb.rename(columns = {'origin_number':'receive_number','id_number':'rcid_number'}, inplace = True)


    newRevenueDFc = pd.merge(newRevenueDFb, devicesDFtempb, how="left",on="receive_number")
    newRevenueDFc.dropna(subset=["rcid_number"], axis=0, inplace=True)
    newRevenueDFc.tail()

    customersdftemp.rename(columns = {'id_number':'rcid_number','customer_type':'rccustomer_type'}, inplace = True)

    #CREATE A DATAFRAME WITH CUSTOMER TYPES and durations 
    newRevenueDFd = pd.merge(newRevenueDFc, customersdftemp, how="left",on="rcid_number")
    newRevenueDFd.dropna(subset=["rccustomer_type","ogcustomer_type","rccustomer_type"], axis=0, inplace=True)
    newRevenueDFd = newRevenueDFd.reset_index(drop=True)
    

    #CALCULATE CALL COSTS AND CREATE REVENUE COLUMN  START HERE 
    baseCharges=[] 
    #clean tower column
    for call in newRevenueDFd['ogcustomer_type'].str.lower():
        #callType.append(call)
            call = call 
            if call == 'normal':
                basecharge=200
                baseCharges.append(basecharge)
            elif call== 'vip':
                basecharge=500
                baseCharges.append(basecharge)
            elif call == 'basic':
                basecharge=0
                baseCharges.append(basecharge)

    #print(baseCharges)
    baseChargesColumn=pd.DataFrame(baseCharges)
    baseChargesColumn.rename(columns= {0:'baseCharge'}, inplace = True)

    #add base charges to Dataframe 
    newRevenueDFd['basecharge']=baseChargesColumn['baseCharge']

        #add call cost column 
    #weght columns and use numbers 

    callTypesog=[] 

    callTypesrc = []

    callCost=[]

    for call in newRevenueDFd['ogcustomer_type'].str.lower():
        #callType.append(call)
              callTypesog.append(call)

    for call in newRevenueDFd['rccustomer_type'].str.lower():
        #callType.append(call)
            callTypesrc.append(call)

    for i in range(len(callTypesog)):
        
        if callTypesog[i] =="vip":
            receiver=callTypesrc[i]
            if receiver == "basic":
                cost= 0.05
                callCost.append(cost)
            elif receiver== "normal":
                cost= 0.05
                callCost.append(cost)
            elif receiver == "vip":
                cost= 0
                callCost.append(cost)
        if callTypesog[i] == "normal":
            receiver=callTypesrc[i]
            if receiver == "basic":
                cost= 0.15
                callCost.append(cost)
            elif receiver == "normal":
                cost= 0.15
                callCost.append(cost)
            elif receiver == "vip":
                cost= 0.15
                callCost.append(cost)

        if callTypesog[i] == "basic":
            receiver=callTypesrc[i]
            if receiver == "basic":
                cost= 0.30
                callCost.append(cost)
            elif receiver=="normal":
                cost= 0.30
                callCost.append(cost)
            elif receiver == "vip":
                cost= 0.30
                callCost.append(cost)

    callCostDF=pd.DataFrame(callCost)
    callCostDF.rename(columns= {0:'cost'}, inplace = True)

    # add cost to dataframe and 
    newRevenueDFd['cost'] = callCostDF['cost']
    newRevenueDFd['callcharge']= (newRevenueDFd['cost']*newRevenueDFd['duration'])+ newRevenueDFd['basecharge']

    #-	Total Revenue per customer DONE
    totalRevenuePC=newRevenueDFd.groupby('ogid_number')['callcharge'].sum().sort_values(ascending=False)
    totalRevenuePC = pd.DataFrame(totalRevenuePC)
    #regionscalledPC.rename(columns = {'ogid_number':'No. Calls'}, inplace = True)
    totalRevenuePC = totalRevenuePC.reset_index(drop=False)
    totalRevenuePC.rename(columns = {'ogid_number':'id_number','callcharge':'Revenue'}, inplace = True)
    totalRevenuePC['id_number'] = totalRevenuePC['id_number'].astype('int')
    totalRevenuePC['Revenue'] = totalRevenuePC['Revenue'].abs()
    totalRevenuePC.head().style.set_caption("Total Revenue per customer")

    print("Total Revenue Per Customer\n",totalRevenuePC,"\n\n")
    # END CODE HERE
    return totalRevenuePC


#Run functions 
num_customers_per_customer_region(customersdf)
num_devices_per_customer_region(devicesDf)
num_towers_per_customer_region(towersdf)
num_calls_received_per_customer_region(callsdf)
num_calls_made_per_customer_region(callsdf,towersdf)
revenue_per_customer_region(devicesDf,customersdf)
num_devices_per_customer(customersdfb,devicesDfb)
num_calls_per_customer(devicesDfb,customersdfb,towersdf)
num_regions_called_per_customer(devicesDfb,customersdfb,towersdf,callsdf)
num_towers_called_per_customer(devicesDfb,customersdfb,callsdf,towersdf)
total_revenue_per_customer(devicesDfb,customersdfb,callsdf,towersdf)