from ConcurrencyControlManager.Algorithms.AbstractAlgorithm import AbstractAlgorithm
from Interface import Response, Action, Transaction


class Version:
    def __init__(self, value: str, w_ts: int,r_ts:int):
        self.content = value
        self.w_ts = w_ts
        self.r_ts = r_ts


class MVCC(AbstractAlgorithm):
    def __init__(self):
        # Format versions: {data_item (str): list of versions (Version)}
        # Version tuple: (content, W-ts, R-ts)
        self.versions = {str:{Version}}

        # Query log to store queries made to the system
        self.query_log = []

        # Timestamps: {Transaction_id: Timestamp}
        self.timestamps = {}
    
    def getTimestamp(self,tx_id) -> int:
        return self.timestamps[tx_id]
    
    def setTimestamp(self,tx_id,ts):
        self.timestamps[tx_id] = ts

    def checkVersion(self,data_item:str) -> bool:
        return data_item in self.versions
    
    def getVersions(self,data_item:str) -> {Version}:
        return self.versions.get(data_item)
        
    def getLatestVersion(self,data_item:str) -> Version:
        versions = self.getVersions(data_item)
        if versions is None:
            return Version(None,0,0)
        else:
            last_idx = len(versions)-1
            return versions[last_idx]

    def addVersion(self,data_item:str,version):
        if self.checkVersion(data_item):   
            versions = self.getVersions(data_item)
            versions.append(version)
        else:
            self.versions[data_item] = [version]

    def read(self,data_item:str,tx_id : int)-> bool:
        last = self.getLatestVersion(data_item)
        r_ts = last[2]
        ts = self.timestamps[tx_id]
        if r_ts < self.timestamps[tx_id]:
            self.addVersion(data_item,Version(last[0],last[1],ts))
        return True
        
    
    def write(self,data_item:str,tx_id:int) -> bool:
        last = self.getLatestVersion(data_item)
        r_ts = last[2]
        w_ts = last[1]
        ts = self.timestamps[tx_id]
        if r_ts > ts or w_ts > ts:
            return False
        else:
            self.addVersion(data_item,(last[0],ts,r_ts))
            return True
         

    def run(self, db_object: int, transaction_id: int) -> None:
        pass

    def validate(self, db_object: int, transactionId:int, action:Action) -> Response:
        if action == 'read':
            return Response(self.read('dataitem',transactionId),transactionId)
        elif action == 'write' :
            return Response(self.write('dataitem',transactionId),transactionId)


    def end(self, transaction_id: int) -> bool:
        pass


