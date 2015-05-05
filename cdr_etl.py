import os, sys, glob
import datetime as dt
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from db_models import *
from distutils.dir_util import mkpath
from cdr_settings import cdr_settings as settings

Session = sessionmaker(bind=settings.engine)
session = Session()

class cdr_log():
    
    def __init__(self):
            pass
        
    def startGenSession(self, ref_date, stream_id): 
        
        newGenSession= GenerateSession (ref_date=ref_date, start_time = dt.datetime.now(), status = "STARTING", stream_id = stream_id)
        session.add(newGenSession)
        session.commit()
        self.genSession=newGenSession
        return newGenSession
    
    def endGenSession(self):
        activeSession=self.genSession 
        activeSession.status="COMPLETED"
        activeSession.end_time=dt.datetime.now()
        diff=activeSession.end_time-activeSession.start_time
        activeSession.duration=diff.seconds
        session.add(activeSession)
        session.commit()
        self.genSession=activeSession
    
    def failedSession(self):
        
        activeSession=self.genSession 
        activeSession.status="FAILED"
        activeSession.end_time=dt.datetime.now()
        diff=activeSession.end_time-activeSession.start_time
        activeSession.duration=diff.seconds
        session.add(activeSession)
        session.commit()
        self.genSession=activeSession
       
       
class cdr_etl():
    
    def __init__(self, cdr_name,ref_date,session):
        
        self.stream=session.query(Stream).filter(Stream.name==cdr_name)
        self.log=cdr_log()
        self.ses= self.log.startGenSession(ref_date,self.stream.id)
        self.in_dir=os.path.join(settings.working_dir, 'in')
        self.load_dir=os.path.join(os.path.join(settings.working_dir, 'load'), self.stream.name)
        self.script_dir=os.path.join(settings.working_dir, 'script')
        self.ref_date= ref_date.strftime("%Y%m%d")
        self.search_term='*'+self.ref_date+'*' #should be passed from db as cdr.search_term
  
    def get_cdrs(self): 
        files= glob.glob(self.in_dir+'/'+self.search_term)[:self.stream.files_per_session]
        #remove loaded files
        #files=remove_loaded(files)
        #check if cdr.is_compressed then uncompress 
        return files
    
    def uncompress():
        pass
    
    def mkdir_p(self, path):

        try:
            os.stat(path)
        except:
            os.mkdir(path)       

        f = file(filename)

    def remove_loaded(self,files):
        #query db and get a list of previously loaded files as loaded_files
        #call move_to_archive(loaded_files)
        #to return an array of files
        pass
    
    def move_to_archive(self):
        #move all files to archive partitioned by date on the files names
        pass
    
    def test(self):
        print self.script_dir
        print self.stream.name
        print self.stream.files_per_session
        print self.stream.no_threads
         
    def distribute(self):
        # create the files -- this will be the array from ls
        files=self.get_cdrs()
        # create dir or in this case array that contain the sub dir or array that amount to the number of thread
        cdr_threads=[[] for i in range(self.stream.no_threads)]
        # allocate spaces for the number of files to go into each thread based on the number of files coming
        i=0
        for file in files:
            cdr_threads[i].append(file)
            i+=1
            if i==self.stream.no_threads:
                i=0
        return cdr_threads
    
    def distribute2(self):
        """@TODO :this method aim to distributes files in such so the the files are in the folders sequecially i.e  folder one contains 
        the first 1-10 files and the last folder contains the last set of files i.e the reminder"""
        
        os.chdir(self.in_dir)
        # create the files -- this will be the array from ls
        files=self.get_cdrs()
        # create dir or in this case array that contain the sub dir or array that amount to the number of thread
        cdr=[[] for i in range(self.stream.no_threads)]
        # allocate spaces for the number of files to go into each thread based on the number of files coming
        interval=self.stream.files_per_session/self.stream.no_threads
        mod_int= self.stream.files_per_session%self.stream.no_threads
        for i in range(self.no_threads):
            for j in range (interval):
                cdr[i].append([])

            if i <= mod_int-1:
                cdr[i].append([])
                print mod_int,i 

        n=0
        for i in range (self.no_threads):
        #     print 'filse', i, len(cdr[i])
        #     print n, n+len(cdr[i])
            cdr[i]=files[n:n+len(cdr[i])]
            n=n+len(cdr[i])

#         for i in range(self.no_threads):
#             print len(cdr[i])
#             print cdr[i]
#             process_cdr (cdr[i], i)
            
    def process_thread(self, files, thread_no):
        print 'for thread',thread_no+1, 'there are ', len(files), 'files' 
        #set the path to put all_data.txt
        #load_dir=self.load_dir+'/'+self.ref_date
        load_dir=os.path.join(self.load_dir, self.ref_date)
        #@TODO: REPLACE THIS MKDIR -P WITH A PYTHON IMPLEMENTAION
        #l=os.system("mkdir -p {0}".format(load_dir))
        os.makedir(load_dir, exist_ok=True)
        #set the name of the final files with thread_no and timestamp/session_number/sequence
        data_file=load_dir+'/all_data_'+str(thread_no+1)+'_'+str(self.ses.id)+'.txt'
        if os.path.exists(data_file):
            os.remove(data_file)
        with open(data_file, 'w') as outfile:
            for i in range(len(files)):
                fname=files[i]
                with open(fname) as infile:
                    for line in infile:
                        line=fname+'|'+self.ref_date+'|'+line
                        outfile.write(line)
        rtn='done {}'.format(thread_no+1)
        #archive all_data.txt
        print rtn
        return rtn
