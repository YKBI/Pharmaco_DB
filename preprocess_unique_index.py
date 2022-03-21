import os,glob,sys
import pandas as pd
from tqdm import tqdm
from itertools import product

if __name__ == "__main__":
    f_list = sorted(glob.glob("/Users/yklee/Desktop/yklee/Pharmaco_DB/Pharmaco_DB_Index/Pharmaco_DB_work_220302/*/*.csv"))
    Donors = set()
    Acceptors = set()
    Hydrophobics = set()
    Pharma = {}
    for f in tqdm(f_list):
        df = pd.read_csv(f)
        tmp_d = set(df["Donor"].tolist())
        tmp_a = set(df["Acceptor"].tolist())
        tmp_p = set(df["Hydrophobe"].tolist())
        Donors = Donors|tmp_d
        Acceptors = Acceptors|tmp_a
        Hydrophobics = Hydrophobics|tmp_p
    Pharma["Donor"] = Donors
    Pharma["Acceptor"] = Acceptors
    Pharma["Hydrophobe"] = Hydrophobics

    tdf = pd.DataFrame.from_dict(Pharma,orient="index").T
    tdf.to_csv("Preproc_unique_index.csv",index=False)

    Hydrophobics = tdf["Hydrophobe"].dropna().tolist()
    Acceptors = tdf["Acceptor"].dropna().tolist()
    Donors = tdf["Donor"].dropna().tolist()
    items = [Hydrophobics,Acceptors,Donors]
    prod_list = list(product(*items))
    print(len(prod_list))
    class_dic = {}
    for i in prod_list:
        idx = prod_list.index(i)
        idx_name = "INDEX_%s"%str(idx).rjust(5,"0")
        class_dic[idx_name] = i

    df = pd.DataFrame.from_dict(class_dic,orient="index").reset_index()
    df.columns = ["Index_Name","Hydrophobe","Acceptor","Donor"]

    df.to_csv("Total_Index_info.csv",index=False)
