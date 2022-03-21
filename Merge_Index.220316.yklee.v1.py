import pandas as pd
import glob
from tqdm import tqdm

def Parse_file(f):
    tmp = {}
    df = pd.read_csv(f)
    for idx,line in df.iterrows():
        hph,hba,hbd = line.values[1],line.values[2],line.values[3]
        tmp[(hph,hba,hbd)] = line.values[5]

    return tmp

if __name__ == "__main__":
    f_list = sorted(glob.glob("*/Make_Index_by_node.csv"))
    my_idx = pd.read_csv("Total_Index_info.csv")
    my_dic = {}
    all_dic = {}
    for f in f_list:
        dirn = f.split("/")[0]
        tmp = Parse_file(f)
        all_dic[dirn] = tmp

    for idx,line in tqdm(my_idx.iterrows(),total=my_idx.shape[0]):
        hph,hba,hbd = line.values[1],line.values[2],line.values[3]
        skey = (hph,hba,hbd)
        i_name = line.values[0]
        id_list = set()
        for i in all_dic:
            p = 0
            tmp = all_dic[i]
            try:
                ids = set(tmp[skey].split(","))
            except:
                p = 1
            if p == 1:
                pass
            else:
                id_list = id_list |ids
        my_dic[i_name] = [hph,hba,hbd,len(id_list),','.join(id_list)]

    df = pd.DataFrame.from_dict(my_dic,orient="index").reset_index()
    df.columns = ["Index_Name","Hydrophobe","Acceptor","Donor","N of ZIDs","ZIDs"]

    df.to_csv("test.csv",index=False)

