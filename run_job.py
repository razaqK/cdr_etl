import  datetime as dt
import multiprocessing as mp
from cdr_etl import cdr_etl
from sqlalchemy.orm import sessionmaker
from cdr_settings import cdr_settings as settings


def main_generate(stream, ref_date):
    cdrProcessor=cdr_etl(stream_name,ref_date)
    files=cdrProcessor.distribute()
    for i in range (len(files)):
        cdrProcessor.process_thread(files[i], i)  
#     cdrProcessor.test()

def process_thread_parallel(files):
    current =mp.current_process()
    cdr_etl=files.pop(len(files)-2)
    thread_no=files.pop(len(files)-1)
#     vd.test()
#     print current.name
#     print os.getpid()
#     print files[len(files)-1]
    rtn=cdr_etl.process_thread(files,thread_no)
    return rtn


def main_generate_parallel(stream, ref_date,session):
    cdrProcessor=cdr_etl(stream_name,ref_date,session)
    files=cdrProcessor.distribute()
#     cdrProcessor.test()
    #append the class and thread no to the files for each thread
    for i in range (len(files)):
        files[i].append(cdrProcessor)
        files[i].append(i)
    p = mp.Pool(cdrProcessor.stream.no_threads) 
    result=p.map_async(process_thread_parallel , files)
#     result.wait()
    ppp=result.get()
#cdrProcessor.process_thread3(files)


###############################################################################################################
Session = sessionmaker(bind=settings.engine)
session = Session()

ref_date=dt.datetime(2015,01,10)
stream_name='voice'
 
# main_generate(stream_name, ref_date)
main_generate_parallel(stream_name, ref_date,session)
