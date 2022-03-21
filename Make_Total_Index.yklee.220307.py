import logging,os,sys
import multiprocessing as mp
import pandas as pd
from itertools import product
def my_logger_set():
    logger = logging.getLogger(__name__)
    formatter = logging.Formatter('[%(asctime)s][%(levelname)s|%(filename)s:%(lineno)s] >> %(message)s')
    streamHandler = logging.StreamHandler()
    fileHandler = logging.FileHandler("my_python.log")
    streamHandler.setFormatter(formatter)
    fileHandler.setFormatter(formatter)
    logger.addHandler(streamHandler)
    logger.addHandler(fileHandler)
    logger.setLevel(level=logging.DEBUG)
    return logger

class Make_Total_Index:
    def __init__(self,inp,logger):
        self.logger = logger
        self.df = pd.read_csv(inp)
        self.logger.info("Reading CSV file")
        self.cols = self.df.columns
        self.cols1 = ["Hydrophobe","Acceptor","Donor"]
    def make_index(self):
        self.logger.info("START Function \"make_index\"")
        didi = {}
        items = []
        for i in self.cols:
            items.append(self.df[i].dropna().tolist())
        prod_list = list(product(*items))
        for i in prod_list:
            idx = prod_list.index(i)
            class_name = "class_%s"%str(idx)
            didi[class_name] = i
        self.logger.info("FINISH Function \"make_index\"")
        return didi

if __name__ == "__main__":
    logger = my_logger_set()
    logger.info("Script START")
    j = Make_Total_Index("/Users/yklee/Downloads/yklee/Pharmaco_DB/Pharmaco_DB_Index/Preproc_Unique_index.csv",logger)
    tlist = j.make_index()
    df = pd.DataFrame.from_dict(tlist).T.reset_index().rename(columns = {"index":"Total_index",0:"Hydrophobe",1:"Acceptor",2:"Donor"})
    df.to_csv("Pharmacophore_Total_index.csv",index=False,sep='\t')
    logger.info("Script FINISH")
