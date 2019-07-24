import openaq
import pickle
import pandas as pd
import io
import requests

def download(loc,verbose=False,cache='use',cacheonly=None,pagesize=1000,startdate=None):
    """Download data from OpenAQ.
    loc = location
    e.g. loc = 'US Diplomatic Post: Kampala' 
    cache = whether to use the cache (set to 'use', 'only' or 'refresh')
    cacheonly = if set: number of previous data points to save in cache
    pagesize = size of pages to load data"""
    
    olddata = None
    
    if not (cache=='refresh'):
        try:            
            if verbose:
                print("Trying cache")
            olddata = pickle.load(open("openaq_%s.p" % loc,'rb'))
            #olddate = max(olddata.index) #get the newest time we have in cache
            if verbose:
                print("Loaded %d records from cache." % len(olddata))
        except IOError: #has to be changed for Python 3...
            if verbose:
                print("Cache not found")
 
    if olddata is None:
        print("Downloading backed up raw CSV files from OpenAQ AWS. This may take a while")
        #I don't see much alternative to downloading the whole file, as the records are spread throughout the file
        cs = []
#        ts = pd.Timestamp('2015-06-29')
        if startdate is None:
            ts = pd.Timestamp('2015-06-29') #earlier file in openAQ storage
        else:
            if type(startdate)==str:            
                ts = pd.Timestamp(startdate)
            else:
                ts = startdate #assume it's a pd timestamp

        while ts<pd.to_datetime('today')-pd.DateOffset(88): #only need to go to 88 days ago - can use faster API for more recent data
            url=ts.strftime('https://openaq-data.s3.amazonaws.com/%Y-%m-%d.csv')
            if verbose:
                print("Downloading %s" % url)
            s=requests.get(url).content
            try:
                c=pd.read_csv(io.StringIO(s.decode('utf-8')))        
                c = c[c['location']==loc]
                c.utc = pd.to_datetime(c['utc'])
                c.local = pd.to_datetime(c['local'])                
                c = c.rename(columns={"latitude":"coordinates.latitude","longitude":"coordinates.longitude","utc":"date.utc","local":"date.local"})
                c = c.set_index('date.local')              
                cs.append(c)
            except KeyError:
                print("    Failed to parse %s" % url)
                pass
            ts = ts + pd.DateOffset(1)

        if verbose:
            print("Combining %d days worth of records" % len(cs))
        olddata = pd.concat(cs).drop_duplicates()
        if verbose:
            print("%d records retrieved from OpenAQ backup server." % len(olddata))
                
    if cache=='only':
        if verbose:
            print("Not updating cache.")
        return olddata

    page = 0
    data = None
    api = openaq.OpenAQ()#limit=10000)    
    while (True): #keep stepping through all the pages in the API    
        page+=1
        try: #try to read the next page, and concatenate them
            if verbose:
                print("Loading API page %d" % page)
            dfpage = api.measurements(location=loc,limit=pagesize,df=True,page=page)
            dfpage = dfpage.set_index(pd.to_datetime(dfpage.index))
        except KeyError:
            print("End of pagination on page %d" % page)
            break #give up when we can't read any more pages
        if data is None:
            data = dfpage
        else:
            data = pd.concat([dfpage,data]).drop_duplicates()
        #if (min(data.index)<olddate): #TODO: Not sure API returns it in the right order for this to work.
        #    break
        #    #we only need to go as far back as the cached data
            
    if olddata is not None:
        if verbose:
            print("Combining %d cached records with %d API records" % (len(olddata),len(data)))
        data = pd.concat([data,olddata]).drop_duplicates()
        
    if verbose:
        print("Total data set contains %d records" % len(data))
    if cacheonly is not None:        
        pickle.dump(data[-cacheonly:],open('openaq_%s.p' % loc,'wb'))
    else:
        pickle.dump(data,open('openaq_%s.p' % loc,'wb'))
    return data.sort_values(by=['date.utc'])
