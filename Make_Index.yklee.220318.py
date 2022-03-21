import os,sys,glob,logging
import multiprocessing as mp
from itertools import combinations,product
from functools import partial
from multiprocessing import Manager
import pandas as pd
from time import time
def Upload_memory(Cluster_dic,hbd_list,hba_list,hph_list,lines):
	if lines.startswith("ID"):
		return 0
	else:
		pass
	tline = lines.split(",")
	tid = tline[0]
	hbd = tline[1]
	hba = tline[2]
	hph = tline[5]
	Cluster_dic[tid] = [hph,hba,hbd]
	hbd_list[hbd] = 1
	hba_list[hba] = 1
	hph_list[hph] = 1
def Make_Cluster(hbd_list,hba_list,hph_list,clist):
	items = [hph_list.keys(),hba_list.keys(),hbd_list.keys()]
	prod_list = list(product(*items))
	for i in prod_list:
		clist.append(list(i))
def Do_HCluster(Cluster_dic,clist,Final_dict,tid):
	it = Cluster_dic[tid]
	class_id = str(clist.index(it))
	if "Class_%s"%class_id in Final_dict.keys():
		tmp = set(Final_dict["Class_%s"%class_id].split(","))
	else:
		tmp = set()
	tmp.add(tid)
	Final_dict["Class_%s"%class_id] = ','.join(tmp)
def my_logger_set():
	logger = logging.getLogger(__name__)
	formatter = logging.Formatter('[%(asctime)s][%(levelname)s|%(filename)s:%(lineno)s] >> %(message)s')
	streamHandler = logging.StreamHandler()
	fileHandler = logging.FileHandler("Make_index.python.log")
	streamHandler.setFormatter(formatter)
	fileHandler.setFormatter(formatter)
	logger.addHandler(streamHandler)
	logger.addHandler(fileHandler)
	logger.setLevel(level=logging.DEBUG)
	return logger
if __name__ == "__main__":
	logger = my_logger_set()
	manager = Manager()
	Cluster_dic = manager.dict()
	hbd_list = manager.dict()
	hba_list = manager.dict()
	hph_list = manager.dict()
	Final_dict = manager.dict()

	class_name = {}
	class_list = []
	f_list = sorted(glob.glob("*/rdkit_pharmaco.*.csv"))[:2]
	
	tline_list = manager.list()
	Ncpu = mp.cpu_count()
	pool = mp.Pool(Ncpu-4)
	logger.info("START Reading Lines")
	a = time()
	for f in f_list:
		with open(f,"r") as F:
			for line in F.readlines():
				if line.startswith("ID"):
					pass
				else:
					tline_list.append(line.strip())
	b = time() - a
	logger.info("FINISH Reading Lines")
	logger.info("Processing Time : %f"%b)

	logger.info("START Make Pharmacophore Lists")
	a = time()
	func = partial(Upload_memory,Cluster_dic,hbd_list,hba_list,hph_list)
	pool.map(func,tline_list)
	pool.close()
	pool.join()
	b = time() - a
	logger.info("FINISH Make Pharmacophore Lists")
	logger.info("Processing Time : %f"%b)

	logger.info("START Make Index")
	a = time()
	Make_Cluster(hbd_list,hba_list,hph_list,class_list)
	b = time() - a
	logger.info("FINISH Make Index")
	logger.info("Processing Time : %f"%b)
	
	for i in class_list:
		cidx = str(class_list.index(i))
		class_name["Class_%s"%cidx] = i
	class_df = pd.DataFrame.from_dict(class_name,orient="index").reset_index().rename(columns={"index":"Cluster_Name",0:"Hydrophobe",1:"Acceptor",2:"Donor"})
	logger.info("START Arrange ZINC IDs by Index")
	a = time()
	pool = mp.Pool(Ncpu-4)
	func = partial(Do_HCluster,Cluster_dic,class_list,Final_dict)
	pool.map(func,Cluster_dic.keys())
	pool.close()
	pool.join()
	b = time() - a
	logger.info("FINISH Arrange ZINC IDs by Index")
	logger.info("Processing Time : %f"%b)

	logger.info("START Make Out File")
	a = time()
	df = pd.DataFrame.from_dict(Final_dict,orient="index").reset_index().rename(columns={"index":"Cluster_Name",0:"ZIDs"})
	df["N.ZIDs"] = df["ZIDs"].str.split(",").apply(len)
	fin_df = pd.merge(class_df,df,on="Cluster_Name")
	cols = "Cluster_Name,Hydrophobe,Acceptor,Donor,N.ZIDs,ZIDs".split(",")
	fin_df = fin_df[cols]
	fin_df.to_csv("Make_Index_by_node.csv",index=False)
	b = time() - a
	logger.info("FINISH Make Out File")
	logger.info("Processing Time : %f"%b)

