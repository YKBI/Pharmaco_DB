import pandas as pd
import glob
from tqdm import tqdm


def Parse_file(my_idx: pd.DataFrame, tmp: dict, f: str) -> None:
    f = pd.read_csv(f)
    df = pd.merge(f, my_idx, on=["Hydrophobe", "Acceptor", "Donor"])
    for idx, line in df.iterrows():
        idx_name = line.values[6]
        if idx_name in tmp:
            temp = tmp[idx_name]
            temp = temp | set(line.values[5].split(","))
        elif idx_name not in tmp:
            temp = set(line.values[5].split(","))
        else:
            print("Error, Loop Stop")
            break
        tmp[idx_name] = temp

if __name__ == "__main__":
    f_list = sorted(glob.glob("*/Make_Index_by_node.csv"))
    my_idx = pd.read_csv("Total_Index_info.csv")
    my_dic = {}
    for f in tqdm(f_list[:2]):
        dirn = f.split("/")[0]
        Parse_file(my_idx, my_dic, f)
    print("input parsing End")
    for i in my_dic:
        tlen = len(my_dic[i])
        tline = ','.join(my_dic[i])
        my_dic[i] = [tlen, tline]
    df = pd.DataFrame.from_dict(my_dic).T.reset_index()
    df.columns = ["Index_Name", "N of ZIDs", "ZIDs"]
    tdf = pd.merge(my_idx, df, on="Index_Name")
    tdf.to_csv("test3.csv", index=False)
