import time
import random
import mysql.connector
import resources.secrets.local_secrets as local_secrets
import queue
import threading
import requests
from typing import Dict, Any, List

class pullData:
    def __init__(self,id,distance,speed,force,pull_time):
        self.id=id
        self.distance=distance
        self.force=force
        self.speed=speed
        self.pull_time=pull_time
    

    def __repr__(self):
        return f"Pull Data {self.id} Dist: {self.distance} Speed: {self.speed} Force: {self.force}"

class pull:
    
    def __init__(self):
        self.data=[]
        self.id=None
        self.hook=None
        self.team_id=None

#session = requests.Session()

def s_post(url: str, payload: Dict[str, Any],session) -> None:
    rr = session.post(url, json=payload, timeout=2.0)
    rr.raise_for_status()


class simulator:
    def __init__(self):
        self.create_db_connection()
        self.pull_queue=queue.Queue(10000)
        self.pull_thread=threading.Thread(target=self.pull_thread_func,daemon=True)
        self.pull_thread.start()
        self.upload_queue=queue.Queue()
        for i in range(10):
            self.upload_thread=threading.Thread(target=self.upload_thread_func,daemon=True)
            self.upload_thread.start()
        for i in range(10):
            self.pull_queue.put(self.get_pull_data())

    def upload_thread_func(self):
        i=0
        session = requests.Session()
        while True:
            i+=1
            if i%10==0:
                print("Queue Size:"+str(self.upload_queue.qsize()))
            try:
                packet=self.upload_queue.get()
                packet[1].append(session)
                packet[0](*packet[1])
            except queue.Empty:
                pass
            except requests.exceptions.ReadTimeout as e:
                print(e)
                session.close()
                session=requests.session()

    def pull_thread_func(self):
        id=0
        while True:
            try:
                execute_pull=self.pull_queue.get()
                time.sleep(10)
                self.upload_queue.put([s_post,["https://ingest.internationalquarterscale.com/api/ingest/status", {"ts": time.time(), "status": 0, "pull_id": id}]])
                id+=1
                start_time=time.time()
                for data in execute_pull.data:
                    while elapsed_time:=time.time()-start_time<data.pull_time:
                        pass
                    #print(data)
                    if (data.id%10==0):
                        
                        self.upload_queue.put([s_post,["https://ingest.internationalquarterscale.com/api/ingest/data", {
                        "ts": data.pull_time,
                        "speed": round(data.speed, 2),
                        "force": data.force,
                        "distance": round(data.distance, 2),
                        }]])
            except queue.Empty as e:
                pass

    def create_db_connection(self):
        self.db=mysql.connector.connect(
            host=local_secrets.DB_IP,
            user=local_secrets.DB_UNAME,
            password=local_secrets.DB_PWORD,
            database=local_secrets.DB_NAME
        )
        self.cursor=self.db.cursor(dictionary=True)

    def get_pull_data(self):
        sql="SELECT pull_id FROM pulls WHERE final_distance IS NOT NULL"
        self.cursor.execute(sql)
        x=self.cursor.fetchall()
        #print(x)
        count=len(x)
        #print(count)
        id=random.randint(0,count-1)
        pull_id=x[id]["pull_id"]
        print(pull_id)
        sql="SELECT * FROM pull_data WHERE pull_id = %s"
        values=(pull_id,)
        self.cursor.execute(sql,values)
        x=self.cursor.fetchall()
        current_pull=pull()
        for record in x:
            data=pullData(record["data_id"],record["distance"],record["speed"],record["chain_force"],record["pull_time"])
            current_pull.data.append(data)
        print(len(x))
        i=0
        start_time=time.time()
        return current_pull
        # while i <len(x):
        #     elapsed=time.time()-start_time
        #     dt=elapsed-x[i]["pull_time"]
        #     if dt>0:
        #         x[i]["dt"]=dt
        #         print(x[i])
        #         i+=1
        #     else:
        #         pass

def main():
    sim=simulator()


if __name__=="__main__":
    main()
    time.sleep(1000)

