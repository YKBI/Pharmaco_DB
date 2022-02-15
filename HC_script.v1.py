import json,glob,os,sys
import scipy.cluster.hierarchy as shc
import matplotlib
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from scipy.cluster.hierarchy import linkage,dendrogram
import random,multiprocessing
from multiprocessing import Manager
from itertools import combinations,product
from functools import partial
matplotlib.use("Agg")
plt.switch_backend("agg")
def HC_module(afile):
	fname = os.path.basename(afile)
	hp_list =0
	ha_list =0
	hd_list =0
	aro_list =0
	with open(afile) as F:
		json_data = json.load(F)
		for line in json_data["points"]:
			if line["name"] == "Hydrophobic":
				hp_list += 1
			elif line["name"] == "HydrogenAcceptor":
				ha_list += 1
			elif line["name"] == "HydrogenDonor":
				hd_list += 1
	#dics[fname] = [hp_list,ha_list,hd_list] #,aro_list]
	ttt[fname] = [hp_list,ha_list,hd_list]
def make_cluster():
	Cluster_dic = {}
	C_dic = {}
	a = set()
	b = set()
	c = set()
	#d = set()
	for i in ttt:
		a.add(ttt[i][0])
		b.add(ttt[i][1])
		c.add(ttt[i][2])
		#d.add(ttt[i][3])
	items = [a,b,c] #,d]
	print(min(a),max(a))
	print(min(b),max(b))
	print(min(c),max(c))
	#print(min(d),max(d))
	alp = 0
	class_list = []
	prod_list = list(product(*items))
	for i in prod_list:
		class_list.append(list(i))
	for i in ttt:
		it = ttt[i]
		class_id = str(class_list.index(it))
		if "Class_%s"%class_id in Cluster_dic.keys():
			tmp = Cluster_dic["Class_%s"%class_id]
			C_dic["Class_%s"%class_id] = it
		else:
			tmp = []
			C_dic["Class_%s"%class_id] = it
		tmp.append(i)
		Cluster_dic["Class_%s"%class_id] = tmp
	print("Total Num. of Classes: ",len(class_list))
	print(len(Cluster_dic))
	"""
	for i in Cluster_dic:
		print(i,len(Cluster_dic[i]),C_dic[i]) #,Cluster_dic[i])"""
		



if __name__ == "__main__":
	global ttt
	manager = Manager()
	ttt = manager.dict()
	print(ttt)
	f_list = glob.glob("ext_pharma_10m/*.json")
	Ncpu = multiprocessing.cpu_count()
	pool = multiprocessing.Pool(Ncpu-2)
	#func = partial(HC_module,ttt)
	#pool.map(func,f_list)
	pool.map(HC_module,f_list)
	pool.close()
	pool.join()
	#for f in f_list:
	#	HC_module(f)
	make_cluster()


