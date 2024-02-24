from os import listdir as os_listdir
from os import path as os_path

from sqlite3 import connect as sqlcon
from fitz import open as pdf_open

from re import sub

fp_db = 'D:/项目/database/bibliography.db'
dir_path_list = [ 'D:/项目/database/data/book',
                'D:/项目/database/data/paper',
                'D:/项目/database/data/other']
# 可以改成自动遍历所有文件夹

def db_write(fp_db: str = fp_db,
             dir_path_list: list = dir_path_list):
    '''
    文件命名格式: id$bookname$author$

    创建或更新数据库
    '''
    fp_list = [dir_path+'/'+fn
                  for dir_path in dir_path_list
                  for fn in os_listdir(dir_path)]
    # 创建(或连接)数据库
    con = sqlcon(fp_db)
    cur = con.cursor()
    
    # 创建书目每页内容数据表单
    sql = '''CREATE TABLE IF NOT EXISTS
            bibcontent(
            id TEXT,
            page INTEGER,
            content TEXT,
            PRIMARY KEY (id, page))'''
    cur.execute(sql)

    # 创建书目信息数据表单
    sql = '''CREATE TABLE IF NOT EXISTS
            bibinfo(
            id TEXT PRIMARY KEY,
            name TEXT,
            author TEXT,
            path TEXT)'''
    cur.execute(sql)

    # 过滤已存在的书目
    sql_exist_id = ''' SELECT path FROM Bibinfo
                    WHERE id=?
                    '''
    # sql_exist_path = '''SELECT path FROM Bibinfo WHERE path = ?'''
    sql_update_path = '''UPDATE Bibinfo SET path = ? WHERE id = ?'''

    # 根据文件路径筛选文件
    # 但这样并不适用于文件路径更新操作
    # fp_list = [fp for fp in fp_list 
    #            if cur.execute(sql_exist, (fp,)).fetchone() == None]
    
    # 生成书目信息(infolist)
    infolist = []
    # [[id1, name1, author1, path1], [id2, name2, ...], ...]

    # 没有对fp_list进行筛选, 会对目录中的每一个文件都进行操作(旧文件更新, 新文件添加)
    # 当目录中的文件很多时, 这样做消耗比较大
    for fp in fp_list:
        fn = os_path.basename(fp)
        name,_ = os_path.splitext(fn) # 分隔文件名和扩展名
        info = name.split("$") # 以'$'为关键信息分隔符

        id = info[0]
        # 检查id是否存在
        res_exist_id = cur.execute(sql_exist_id, (id,)).fetchone()
        if res_exist_id == None:
            # id不存在时
            info.append(fp)
            infolist.append(info)
        else:
            # id存在时
            path_old, = res_exist_id
            if path_old == fp:
                continue
            else:
                cur.execute(sql_update_path, (fp, id)) # 更新路径
                print('update path\n old:%s\n new:%s'%(path_old, fp)) # 打印路径更新的提示信息

    # # 列表推导式表达
    # infolist = [os.path.basename(fp).replace(".pdf","").split('$')+[fp] 
    #             for fp in fp_list]

    # 插入书目信息数据表单
    sql = "INSERT INTO bibinfo VALUES(?, ?, ?, ?)"
    cur.executemany(sql, infolist)

    # 上传文本内容
    sql = "INSERT INTO bibcontent VALUES(?, ?, ?)"
    n = 1 #正上传的文件序号
    Nfiles = len(infolist)

    print("writing start")
    for id, name, _, path in infolist:

        # fp = "%s.pdf"%item[-1] #默认全是PDF文件
        doc = pdf_open(path)
        # id = item[0]
        # name = item[1]
        pagen = 1
        print('%.2f%% writing: %s'%(n/Nfiles*100, name))

        for page in doc:
            content = page.get_text()
            value = [id, pagen, content]
            cur.execute(sql, value)
            pagen += 1
        n += 1

    # 保存更改并关闭数据库
    con.commit()
    con.close()
    print("writing completed")


def db_query(andkeys: str, column: str = 'content', ids: str = "%"):
    '''
    数据库查询

    Parameters
    ----------
    andkeys: str
        关键词, 用逗号","分隔并关键词, 用"OR"分隔或关键词组
    column: str
        用于筛选的列
    ids: str
        用于通过id筛选文件

    Examples
    --------
    db_query('CdTe, phonon OR smear', 
            ids='9812566910, houzhufengvasp')
    >>> No.
        title
        author
        id
        pages
        file path
        ...
    '''

    # 连接数据库
    con = sqlcon(fp_db)
    cur = con.cursor()

    # 关键词筛选语句
    andkeys = [[word.strip() for word in andwords.split(',')] 
               for andwords in andkeys.split('OR')]
    
    query_and = (" AND\n".join(f"{column} LIKE '%{word}%'" for word in keyslist)
                    for keyslist in andkeys)
    
    query_and_or = "\nOR\n".join(query_and)

    # id筛选语句
    id_or = " OR\n".join("id LIKE '%s'"%word.strip() for word in ids.split(','))

    # 完整的sql语句
    sql = '''
            SELECT T2.name, T2.author, T1.id, T1.pages, T2.path
            FROM (
            SELECT id, GROUP_CONCAT(page, ', ') AS pages
            FROM bibcontent
            WHERE (%s)
            AND (%s)
            GROUP BY id
            ) AS T1
            INNER JOIN bibinfo AS T2
            ON T1.id = T2.id
            '''%(id_or, query_and_or) # GROUP_CONCAT是sqlite3特有的函数
    
    # 使用 'LIKE' 而不是 '=' 筛选 'id' 虽然比较耗时, 但是会有一定的容错性
    # 另一方面 '=' 不好设置默认值, 因为没有 '=任意' 这样的语句
    # 只有使用 'LIKE' 操作符才能使用通配符 '%' 匹配任意字符出现任意次数
    
    print('sql:%s'%sub('\n +', '\n', sql)) # 一种处理三引号```产生的空格的方法

    res = cur.execute(sql)

    item = res.fetchone()
    n=1
    while item:
        print(n, '\n'.join(item), end="\n\n", sep="\n")
        item = res.fetchone()
        n += 1
    if n == 1:
        print("no result")
    con.close()

    
# 待做:
# 删除指定id的所有内容
# 可以输入字符串然后提取关键词(空格分隔and关键词, ;分隔or)

if __name__ == '__main__':


    db_query("electron-phonon")