import datetime as dt

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

