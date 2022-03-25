import sqlite3, os, sys,subprocess
from rdkit import Chem
from rdkit.Chem import AllChem
from tqdm import tqdm
from functools import partial
import multiprocessing

def divide_list(ls: list, n: int):
    if n == 0:
        num = 1
    else:
        num = n
    for i in range(0, len(ls), num):
        yield ls[i:i + num]


def change2sdf_v2(w1 : list,inp : tuple) -> None:
    aid = inp[1]
    asmi = inp[0]
    rmol1 = Chem.MolFromSmiles(asmi)
    rmol2 = Chem.AddHs(rmol1)
    AllChem.EmbedMolecule(rmol2, randomSeed=0xf00d)
    rmol3 = Chem.RemoveHs(rmol2)
    rmol3.SetProp("_Name", aid)
    w = Chem.SDWriter("./%s.sdf" % aid)
    w.write(rmol3)
    w1[aid] = rmol3
    w.close()

    del rmol1, rmol2, rmol3

def bringZINC(cursor : object,inp : tuple) -> tuple:
    cursor.execute("SELECT Index_Name,ZIDs FROM PHARMACO WHERE (Hydrophobe, Acceptor, Donor) = (?,?,?)",inp)
    rows = cursor.fetchall()

    In = rows[0][0]
    ZIDs = rows[0][1]

    return In,ZIDs

def bringSMI(cursor : object,inp : str) -> str:
    cursor.execute("SELECT SMILES FROM PROPS WHERE ZINC_ID=?",(inp,))
    rows = cursor.fetchall()

    smiles = rows[0][0]

    return smiles
if __name__ == "__main__":
    Ncpu = multiprocessing.cpu_count()
    manager = multiprocessing.Manager()

    sdf_list = manager.dict()

    hph = sys.argv[1]
    hba = sys.argv[2]
    hbd = sys.argv[3]
    # bring ZID list
    conn = sqlite3.connect("Pharmaco_DB_idx.db")
    cursorObj = conn.cursor()

    I_Name, ZIDstring = bringZINC(cursorObj,(hph,hba,hbd))
    cursorObj.close()

    if os.path.exists(I_Name):
        pass
    else:
        os.makedirs(I_Name)
    os.chdir(I_Name)
    # Make SDF,
    conn = sqlite3.connect("/home/yklee/yklee/ZINC_INFO.db")
    cursorObj = conn.cursor()

    id_smi = []
    for zid in tqdm(ZIDstring.split(","),desc="Bring SMILES from SQL "):

        smi = bringSMI(cursorObj,zid)
        id_smi.append((smi,zid))
    cursorObj.close()
    func = partial(change2sdf_v2,sdf_list)

    pool = multiprocessing.Pool(Ncpu-2)
    with tqdm(total=len(id_smi),desc="Change SMILES to SDF ") as pbar:
        for _ in tqdm(pool.imap_unordered(func,id_smi)):
            pbar.update()
    pool.close()
    pool.join()

    w = Chem.SDWriter("./%s.sdf"%I_Name)
    for i in tqdm(sdf_list,desc="Make Total SDF "):
        rmol = sdf_list[i]
        rmol.SetProp("_Name",i)
        w.write(rmol)
    w.close()

    try:
        subprocess.call("/home/yklee/yklee/ex_Tools/pharmitserver pharma -in %s.sdf -out %s.json"%(I_Name,I_Name))
    except:
        print("Pharmit Operate Error , Plz Check pharmit")
        pass