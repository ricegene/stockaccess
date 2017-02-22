
import pandas as pd
import pypyodbc
import os.path
import os
import re
import time

rootdir="E:/py/data/mainbasic"
dbegin = 1990  # 处理数据的开始年份
dend = 2016  # 处理数据的结束年份（不包括这一年）


def combine(dbegin,dend):

    i = range(dbegin,dend)
    for thisyear in i:
        GrossRatio = pd.read_table(rootdir + '/origin/GrossRate/%s.txt' % thisyear, names=['Stkcode', 'GrossRate'])
        DebtRatio = pd.read_table(rootdir + '/origin/DebtRatio/%s.txt' % thisyear, names=['Stkcode', 'DebtRatio'])
        NetAsset = pd.read_table(rootdir + '/origin/NetAsset/%s.txt' % thisyear, names=['Stkcode', 'NetAsset'])
        NetProfit = pd.read_table(rootdir + '/origin/NetProfit/%s.txt' % thisyear, names=['Stkcode', 'NetProfit'])
        NetRate = pd.read_table(rootdir + '/origin/NetRate/%s.txt' % thisyear, names=['Stkcode', 'NetRate'])
        OpenFlow = pd.read_table(rootdir + '/origin/OpenFlow/%s.txt' % thisyear, names=['Stkcode', 'OpenFlow'])
        OutStanding = pd.read_table(rootdir + '/origin/OutStanding/%s.txt' % thisyear, names=['Stkcode', 'OutStanding'])
        SharehldNum = pd.read_table(rootdir + '/origin/SharehldNum/%s.txt' % thisyear, names=['Stkcode', 'SharehldNum'])

        NetAsset = NetAsset.set_index('Stkcode')
        NetProfit = NetProfit.set_index('Stkcode')
        GrossRatio = GrossRatio.set_index('Stkcode')
        NetRate = NetRate.set_index('Stkcode')
        DebtRatio = DebtRatio.set_index('Stkcode')
        OutStanding = OutStanding.set_index('Stkcode')
        OpenFlow = OpenFlow.set_index('Stkcode')
        SharehldNum = SharehldNum.set_index('Stkcode')

        hb = pd.merge(NetAsset.drop_duplicates(), NetProfit.drop_duplicates(), left_index=True, right_index=True,how='outer').fillna(0)
        hb = pd.merge(hb, GrossRatio.drop_duplicates(), left_index=True, right_index=True, how='outer').fillna(0)
        hb = pd.merge(hb, NetRate.drop_duplicates(), left_index=True, right_index=True, how='outer').fillna(0)
        hb = pd.merge(hb, DebtRatio.drop_duplicates(), left_index=True, right_index=True, how='outer').fillna(0)
        hb = pd.merge(hb, OutStanding.drop_duplicates(), left_index=True, right_index=True, how='outer').fillna(0)
        hb = pd.merge(hb, OpenFlow.drop_duplicates(), left_index=True, right_index=True, how='outer').fillna(0)
        hb = pd.merge(hb, SharehldNum.drop_duplicates(), left_index=True, right_index=True, how='outer').fillna(0)
        hb = hb.reset_index(drop=False)

        pd.DataFrame(hb).to_csv(rootdir + '/process/combied/%s' % thisyear + '.csv', index=False)

def tmpstkname():
    seekdir = rootdir + '/origin/NetAsset/'
    fw = open(rootdir + "/process/tmpname.txt", "a")
    for dirpath, dirnames, filenames in os.walk(seekdir):
        for f in filenames:
            fo = open(seekdir + f, "r")
            while True:
                line = fo.readline()
                if line and line.split():
                    line = line.strip('\n')
                    line = line.split("\t")
                    code = str(line[0])
                    fw.write(code + '\n')
                    # print code
                else:
                    fo.close()
                    break
            else:
                continue

    fw.close()
    df = pd.read_table(rootdir + "/process/tmpname.txt", names=['Stkcode'])
    df.drop_duplicates('Stkcode').to_csv(rootdir + "/process/tmpstkname.txt", index=False, header=False)
    os.remove(rootdir + "/process/tmpname.txt")

def split(tyear):#拆数据，按股票代码
    a = pd.read_table(rootdir + "/process/tmpstkname.txt", header=None)
    a = list(a.ix[:, 0])
    f = open(rootdir + "/process/combied/%s.csv" % tyear)
    counter = 0
    doc = []
    # 将各年数据折成  按股票类类的数据，以附加方式，写进txt
    while True:
        line = f.readline()
        if not line:
            break
        lines = list(line.replace("\n", "").split(","))
        docc = (lines[0], lines[1], lines[2], lines[3], lines[4], lines[5], lines[6], lines[7], lines[8])
        doc = [str(tyear) + "1231", lines[1], lines[2], lines[3], lines[4], lines[5], lines[6], lines[7], lines[8]]

        for j in a:
            if j == docc[0]:
                with open(rootdir + "/process/split/%s.txt" % j, "a") as ff:
                    ff.write(str(doc) + '\n')
        counter = counter + 1
    f.close()


def database_create(tbn):#若数据库文件不存在，则创建access数据库文件以及whois表
    if os.path.exists(rootdir+'/dest/mainbasic.mdb') == False:  # 检查是否存在数据库文件
        connection = pypyodbc.win_create_mdb(rootdir+'/dest/mainbasic.mdb')  # 创建一个新的Access数据库文件，并返回该数据库文件的连接。
    conn = pypyodbc.win_connect_mdb(rootdir+'/dest/mainbasic.mdb')  # 或者你也可以连到先有的Access数据库文件
    cur = conn.cursor()
    #创建以股票代码为名字的数据表
    cur.execute('CREATE TABLE %s (StockDate Date,NetAsset FLOAT,NetProfit FLOAT,GrossRate FLOAT,NetRate FLOAT,DebtRatio FLOAT,OutStanding FLOAT,OpenFlow FLOAT,SharehldNum FLOAT);'%str(tbn))

    cur.commit()
    conn.close()

def database_insert(tbn):#若数据库文件不存在，则创建access数据库文件以及whois表
    conn = pypyodbc.win_connect_mdb(rootdir+'/dest/mainbasic.mdb')  # 或者你也可以连到先有的Access数据库文件
    cur = conn.cursor()

    #插入数据
    f = open(rootdir+"/process/split/%s.TXT" % tbn)
    while True:
        line = f.readline()
        if not line:
            break
        mode = re.compile(r'\d+\.?\d*')
        lines = mode.findall(line)

        cyy = lines[0][0:4]
        cmm = lines[0][4:6]
        cdd = lines[0][6:]
        cres = str(cyy) + '-' + cmm + '-' + cdd + ' 00:00:00'
        cres = time.strftime("%Y-%m-%d", time.strptime(cres, "%Y-%m-%d %H:%M:%S"))
        #print tbn,str(cres)

        # print tbn, lines[0], lines[1], lines[2], lines[3], lines[4], lines[5], lines[6], lines[7], lines[8]

        cur.execute('INSERT INTO %s(StockDate,NetAsset,NetProfit,GrossRate,NetRate,DebtRatio,OutStanding,OpenFlow,SharehldNum) VALUES(?,?,?,?,?,?,?,?,?)' %(tbn), (cres,lines[1],lines[2],lines[3],lines[4],lines[5],lines[6],lines[7],lines[8]))

    cur.commit()
    conn.close()


if __name__ == "__main__":
    starttime=time.clock()

    __import__('shutil').rmtree(rootdir + '/process/')
    __import__('shutil').rmtree(rootdir + '/dest/')
    time.sleep(10)
    if not os.path.exists(rootdir + "/dest/"):
        os.makedirs(rootdir + "/dest/")
    if not os.path.exists(rootdir + "/process/"):
        os.makedirs(rootdir + "/process/")
    if not os.path.exists(rootdir + "/process/combied/"):
        os.makedirs(rootdir + "/process/combied/")
    if not os.path.exists(rootdir + "/process/split/"):
        os.makedirs(rootdir + "/process/split/")

    combine(dbegin,dend)#合并数据

    tmpstkname()#利用源 数据生成股票代码列表文件

    for jk in range(dbegin,dend):
        split(jk)#折成  按股票代码的数据文件

    tmpstkname()#创建临时的股票代码文件

#建库和表
    ab = pd.read_table(rootdir + "/process/tmpstkname.txt", header=None)
    codelist = list(ab.ix[:, 0])
    for k in codelist:
        database_create(k)#创建库和表

    for ik in codelist:
        database_insert(ik)#插入数据

    endtime = time.clock()
    print U'运行耗时:%s  second' % round((starttime - endtime), 2)


