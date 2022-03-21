import os,sys,glob,logging
import multiprocessing as mp
from itertools import combinations,product
from functools import partial
from multiprocessing import Manager
import pandas as pd
from time import time
from tqdm import tqdm
def my_logger_set():
	logger = logging.getLogger(__name__)
	formatter = logging.Formatter('[%(asctime)s][%(levelname)s|%(filename)s:%(lineno)s] >> %(message)s')
	streamHandler = logging.StreamHandler()
	fileHandler = logging.FileHandler("Make_index.python2.log")
	streamHandler.setFormatter(formatter)
	fileHandler.setFormatter(formatter)
	logger.addHandler(streamHandler)
	logger.addHandler(fileHandler)
	logger.setLevel(level=logging.DEBUG)
	return logger
def Parse_file(my_idx,tmp,f) -> None:
	print(f)
	f = pd.read_csv(f)
	df = pd.merge(f,my_idx,on=["Hydrophobe","Acceptor","Donor"])
	for idx, line in tqdm(df.iterrows(),total=df.shape[0]):
		zid = line.values[0]
		idx_name = line.values[9]
		if idx_name in tmp.keys():
			temp = tmp[idx_name]
		elif idx_name not in tmp.keys():
			temp = set()
		else:
			print("Error, Loop Stop")
			break
		temp.add(zid)
		tmp[idx_name] = temp
if __name__ == "__main__":
	logger = my_logger_set()
	tmp_dic = {}
	f_list = sorted(glob.glob("*/rdkit_pharmaco.*.csv"))[:2]
	my_idx = pd.read_csv("Total_Index_info.csv")

	logger.info("START Reading Lines")
	a = time()
	for f in f_list:

		Parse_file(my_idx,tmp_dic,f)



	b = time() - a
	logger.info("FINISH Reading Lines")
	logger.info("Processing Time : %f"%b)

	for i in tqdm(tmp_dic):
		tlen = len(tmp_dic[i])
		tline = ','.join(tmp_dic[i])
		tmp_dic[i] = [tlen,tline]

	df = pd.DataFrame.from_dict(tmp_dic,orient="index").reset_index()
	df.columns = ["Index_Name","N of ZIDs","ZIDs"]
	tdf = pd.merge(my_idx,df,on="Index_Name")
	tdf.to_csv("test.csv",index=False)

