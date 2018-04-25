# written by: Kevin Pielacki
# tested by: Kevin Pielacki


import pymysql
import threading
import tweepy
from tweepy import OAuthHandler
from config import *


auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)
conn = pymysql.connect(**DB_CONFIG)
cur = conn.cursor()


def upsert_df(table, df):                                       
    '''UPSERTS DataFrame to specified table.                          
                                                                      
    Args:                                                             
        table (str): SQL table name to UPSERT to.                     
        df (DataFrame): Pandas DataFrame containing new data entries. 
    '''                                                               
    # Table columns                                                   
    cols = list(df.columns)                                           
    entries = []                                                      
    # Values to insert                                                
    for idx, row in df.iterrows():                                    
        # Remove the "u" flag for unicode entries                     
        row_entries_str = map(str, row.tolist())                      
        entries.append(str(tuple(row_entries_str)))                   
        # Duplicate key update                                        
        conditions = ['{0}=VALUES({0})'.format(col) for col in cols]  
                                                                      
    base_query = '''                                                  
    INSERT INTO {__table__}                                           
        ({__cols__})                                                  
    VALUES                                                            
        {__entries__}                                                 
    ON DUPLICATE KEY UPDATE                                           
        {__conditions__};                                             
    '''                                                               
    query = base_query.format(                                        
        __table__=table,                                              
        __cols__=','.join(cols),                                      
        __entries__=','.join(entries),                                
        __conditions__=','.join(conditions)                           
    )                                                                 
    cur.execute(query)                                        

                                                                             
def twitter_collection_deamon(text, sym, period_dur=3600):                        
    '''Runs twitter api term search every period duration.    
    Args:                                                                    
        text (str): term to search
        symbol (str): stock symbol for related search
        period_dur (float): Time to wait between each request                
            Default to 1 hour                                                
    '''                                                                      
    api = tweepy.API(auth)
    while True:                                                              
        latest = tweepy.Cursor(api.search, q=text).items(1000)
        dates = []
        retweet_count = []
        for item in latest:
            dates.append(item.created_at)
            retweet_count.append(item.retweet_count)
        df = pd.DataFrame({
            'dateid': dates,
            'sym': [sym] * len(dates),
            'retweet_count': retweet_count
        })


        with lock:                                                           
            print('  Inserting new data for {}'.format(text))            
            upsert_df('twitter_data', df)                     
            print('    * SUCCESS!')                                          
        time.sleep(period_dur)                                               
                                                                             
                                                                             
def init_deamons(thread_tasks, target_task):                                 
    '''Starts target_task for each provided thread_tasks.                    
                                                                             
    Args:                                                                    
        thread_tasks (tuple): Contains arguments for target_task             
            symbol (str): stock symbol for related search
            text (tuple): Search text
            period_dur (float): Time to wait between each request            
    '''                                                                      
    print('Initalizing data collection threads')                             
    threads = []                                                             
    msg_temp = '  {} scheduling {} ({}) collection every {} seconds'              
    for idx, thread_task in enumerate(thread_tasks):                         
        text, sym, period = thread_task                                           
        daemon_name = 'daemon_' + str(idx+1)                                 
        print(msg_temp.format(daemon_name, text, sym, period))         
        task = threading.Thread(                                             
            target=target_task,                                              
            name=daemon_name,                                                
            args=(thread_task)                                               
        )                                                                    
        task.setDaemon(True)                                                 
        threads.append(task)                                                 
    # Start all threads                                                      
    print('##### Data Collection #####')                                     
    [task.start() for task in threads]                                       
                                                                             
    # Keep alive                                                             
    while True:                                                              
        pass                                                                 
                                                                             
                                                                             
if __name__ == '__main__':                                                   
    thread_tasks = (                                                     
        ('altaba', 'AABA', 3600),
        ('apple', 'AAPL', 3600),                                          
        ('amd', 'AMD', 3600),
        ('amazon','AMZN', 3600),                                           
        ('citi', 'C', 3600),
        ('intel', 'INTC', 3600),                                             
        ('microsoft', 'MSFT', 3600),
        ('google', 'GOOGL', 3600),                                         
        ('google', 'GOOG', 3600),
        ('verizon','VZ', 3600)                                             
    )                                                                    
    thread_tasks = (thread_tasks)                                        
    init_deamons(thread_tasks, twitter_collection_deamon)                     
