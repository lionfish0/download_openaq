import openaq
import pickle
import pandas as pd

def download(loc,verbose=False,cache='use',cacheonly=None):
    """Download data from OpenAQ.
    loc = location
    e.g. loc = 'US Diplomatic Post: Kampala' 
    cache = whether to use the cache.   
    cacheonly = if set: number of previous data points to save in cache"""
    
    try:
        if verbose:
            print("Trying cache")
        olddata = pickle.load(open("openaq_%s" % loc,'rb'))
        olddate = max(olddata.index) #get the newest time we have in cache
        if verbose:
            print("Loaded %d records from cache." % len(olddata))
    except IOError: #has to be changed for Python 3...
        if verbose:
            print("Cache not found")
        olddata = None
        olddate = pd.Timestamp(year=1970,month=1,day=1)

    if cache=='only':
        if verbose:
            print("Not updating cache.")
        return olddata
        
    page = 0
    data = None
    while (True): #keep stepping through all the pages in the API    
        page+=1
        try: #try to read the next page, and concatenate them
            if verbose:
                print("Loading API page %d" % page)
            api = openaq.OpenAQ(limit=10000)
            dfpage = api.measurements(location=loc,limit=1000,df=True,page=page)
        except KeyError:
            break #give up when we can't read any more pages
        if data is None:
            data = dfpage
        else:
            data = pd.concat([dfpage,data]).drop_duplicates()
        if (min(data.index)<olddate):
            break
            #we only need to go as far back as the cached data
            
    if olddata is not None:
        if verbose:
            print("Combining %d cached records with %d API records" % (len(olddata),len(data)))
        data = pd.concat([data,olddata]).drop_duplicates()
        
    if verbose:
        print("Total data set contains %d records" % len(data))
    if cacheonly is not None:        
        pickle.dump(data[-cacheonly:],open('openaq_%s' % loc,'wb'))
    else:
        pickle.dump(data,open('openaq_%s' % loc,'wb'))
    return data
