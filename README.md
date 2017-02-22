# stockaccess
put finance data to Access, then Foxtrader and WInstcock can use 

将上市公司分年度的财务数据，导入access数据库中，并被飞狐交易师，金字塔等股票软件调用

 清晰展示各上市公司的历年主要财务情况，并进行各种统计
 
参数1 ，rootdir,是数据源的保存地，其下有各类分年度的财务数据，为TXT格式

参数2，dbegin,dend是源数据的开始年份和终止年份，注意0标起点，dend=2016,是指处理到2015年

处理过程中会在rootdir中建立process     dest目录，最终的access数据库在dest目录中,每次处理前会清除  除了原始数据之外的数据

其他过程如下，1，删除上次数据，2创建process和dest目录 3合并分年数据  4生成股票代码文件 5拆分股票数据 6建库，6插入数据


