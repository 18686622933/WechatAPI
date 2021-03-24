#!/usr/bin/env python3
# -*- coding:utf-8 -*-


"""
更新说明：


更换部门和人员的同步方式为全量替换
方法：
先上传含有准确信息的CSV到企业微信，并返回上传文件的media_id，
再根据返回的media_id调用企业微信接口完成对已有数据的全量覆盖，

其中要注意：
文件中不存在、通讯录中存在的部门，当部门下仍有成员或子部门时，暂时不会删除，当下次导入成员把人从部门移出后自动删除
"""


import requests
import json
import cx_Oracle
import pymysql
import pymssql
import sqlite3
import time
import pandas

""" db connect info """
connect = {
    '教务': {'dbtype': 'oracle', 'account': 'zfxfzb/zfsoft_hqwy@orcl', 'is_sysdba': 0},
    '学工': {'dbtype': 'sqlserver', 'account': 'lx/Hqwy@edu2016/192.168.2.78', 'is_sysdba': 0},
    '迎新': {'dbtype': 'sqlserver', 'account': 'lx/Hqwy@edu2016/192.168.2.78', 'is_sysdba': 0},
    '中心库': {'dbtype': 'oracle', 'account': 'dbm/comsys123@dbm', 'is_sysdba': 0},
    '招生': {'dbtype': 'sqlserver', 'account': 'lx/Hqwy@edu2016/192.168.2.78', 'is_sysdba': 0},
    '离校': {'dbtype': 'sqlserver', 'account': 'lx/Hqwy@edu2016/192.168.2.78', 'is_sysdba': 0},
    '人事': {'dbtype': 'sqlserver', 'account': 'lx/Hqwy@edu2016/192.168.2.78', 'is_sysdba': 0},
    '数据湖': {'dbtype': 'oracle', 'account': 'dbm/comsys123/dbm', 'is_sysdba': 0},
}

""" base function """


def pp(data: dict):
    """pretty print 格式化打印json"""
    ppdata = json.dumps(data, indent=4, separators=(',', ':'), ensure_ascii=False)
    print(ppdata)
    return ppdata


def toCSV(data, columns, filename):
    """数据库fetchall()结果转CSV"""
    df = pandas.DataFrame([list(i) for i in data], columns=columns)
    # print(df)
    try:
        df.to_csv(filename, index=False, encoding='utf_8_sig')  # 不显示行号，utf8编码
        print("数据已输出至%s" % filename)
        return filename
    except:
        print("输出到CSV出错")
        return None


def timer(function):
    """
    装饰器函数timer
    :param function:想要计时的函数
    :return:
    """

    def wrapper(*args, **kwargs):
        time_start = time.time()
        res = function(*args, **kwargs)
        cost_time = time.time() - time_start
        print("【%s】运行时间：【%s】秒" % (function.__name__, cost_time))
        return res

    return wrapper


"""数据库类，直接使用函数完成增删改查"""


class Database:
    def __init__(self, dbtype, account: str, is_sysdba=0):
        self.dbtype = dbtype
        self.account = account
        self.is_sysdba = is_sysdba

        if dbtype == 'oracle':
            if is_sysdba:
                self.connect = cx_Oracle.connect(account, mode=cx_Oracle.SYSDBA)
            else:
                self.connect = cx_Oracle.connect(account)
        elif dbtype == 'mysql':
            account = account.split('/')
            self.connect = pymysql.connect(host=account[-1], user=account[0], password=account[1])
        elif dbtype == 'sqlserver':
            account = account.split('/')
            self.connect = pymssql.connect(host=account[-1], user=account[0], password=account[1])
        elif dbtype == 'sqlite':
            self.connect = sqlite3.connect(self.account)

    def connClose(self):
        """创建类时会直接创建数据库连接，在执行完操作之后需要用该函数关闭数据库连接"""
        self.connect.commit()
        self.connect.close()

    def signin(self, dbtype, account: str, is_sysdba=0):
        """也可手动得到数据库连接的类"""
        if dbtype == 'oracle':
            if is_sysdba:
                conn = cx_Oracle.connect(account, mode=cx_Oracle.SYSDBA)
            else:
                conn = cx_Oracle.connect(account)
        elif dbtype == 'mysql':
            account = account.split('/')
            conn = pymysql.connect(host=account[-1], user=account[0], password=account[1])
        elif dbtype == 'sqlserver':
            account = account.split('/')
            conn = pymssql.connect(host=account[-1], user=account[0], password=account[1])
        elif dbtype == 'sqlite':
            conn = sqlite3.connect(self.account)
        else:
            return None

        return conn

    def query(self, sql) -> list:
        """查询数据，返回全部结果"""
        cursor = self.connect.cursor()
        cursor.execute(sql)
        result = cursor.fetchall()
        cursor.close()

        return result

    def delete(self, sql):
        cursor = self.connect.cursor()
        cursor.execute(sql)
        cursor.close()

    def update(self, sql):
        """修改并查询修改结果，修改成功则返回True"""
        cursor = self.connect.cursor()
        # update
        cursor.execute(sql)
        cursor.close()
        self.connect.commit()

        # 查询比对修改结果
        cursor = self.connect.cursor()
        select_sql, value = self.updata2select(sql)
        cursor.execute(select_sql)
        result = cursor.fetchall()
        cursor.close()

        # print(result[0][0])
        # print(value)
        if result and result[0][0] == value:
            return True
        else:
            return False

    @staticmethod
    def updata2select(updata_sql) -> tuple:
        """将uptdata语句转换为select语句，用于修改后的查询"""
        words = updata_sql.split()
        upper_words = list(map(lambda x: x.upper(), words))

        if 'UPDATE' in upper_words and 'SET' in upper_words and 'WHERE' in upper_words:
            table = words[upper_words.index('UPDATE') + 1]
            conditional = ' '.join(words[upper_words.index('WHERE') + 1:])
            set_info = words[upper_words.index('SET') + 1]
            key = set_info.split('=')[0]
            value = set_info.split('=')[1].replace('\'', '')

            select_sql = "SELECT %s FROM %s WHERE %s" % (key, table, conditional)
            return select_sql, value


"""企业微信类，直接使用函数完成对应请求"""


class Wechat:
    def __init__(self):
        self.corpid = "wwfdfdbe459f6eded7"  # 企业id
        self.corpsecret = "mjF5UDXrO5dbdK3kT0Tm2_OGxg4HbXZjFai5ukYfHOE"  # 通讯录secret
        self.token_url = "https://qyapi.weixin.qq.com/cgi-bin/gettoken?corpid=%s&corpsecret=%s"  # 获取token的地址，需要get请求
        self.token_response = requests.get(url=self.token_url % (self.corpid, self.corpsecret))
        self.getToken = self.token_response.json().get('access_token')
        if self.getToken:
            print("token获取成功:", self.getToken)
        else:
            print("token获取失败，请检查:", self.token_response.json())

        self.get_department_url = "https://qyapi.weixin.qq.com/cgi-bin/department/list?access_token=%s&id=%s"  # get
        self.del_department_url = "https://qyapi.weixin.qq.com/cgi-bin/department/delete?access_token=%s&id=%s"  # get
        self.create_department_url = "https://qyapi.weixin.qq.com/cgi-bin/department/create?access_token=%s"  # post
        self.update_department_url = "https://qyapi.weixin.qq.com/cgi-bin/department/update?access_token=%s"  # post

        self.get_staff_url = "https://qyapi.weixin.qq.com/cgi-bin/user/get?access_token=%s&userid=%s"  # get
        self.del_staff_url = "https://qyapi.weixin.qq.com/cgi-bin/user/delete?access_token=%s&userid=%s"  # get
        self.create_staff_url = "https://qyapi.weixin.qq.com/cgi-bin/user/create?access_token=%s"  # post
        self.update_staff_url = "https://qyapi.weixin.qq.com/cgi-bin/user/update?access_token=%s"  # post

        self.cover_deparment_url = "https://qyapi.weixin.qq.com/cgi-bin/batch/replaceparty?access_token=%s"  # post
        self.cover_staff_url = "https://qyapi.weixin.qq.com/cgi-bin/batch/replaceuser?access_token=%s"  # post

        self.upload_url = "https://qyapi.weixin.qq.com/cgi-bin/media/upload?access_token=%s&type=file"  # post
        self.get_media_url = "https://qyapi.weixin.qq.com/cgi-bin/media/get?access_token=%s&media_id=%s"  # get

    """公用函数"""

    @staticmethod
    def printResult(operation, data, response):
        """打印操作结果：根据返回Response类中的errcode值判断操作是否成功，成功则返回操作名+成功+数据内容，否则返回Response类及数据内容"""
        dresponse = response.json()
        if dresponse.get('errcode') == 0:
            print("%s完成" % operation, data)
            return True
        else:
            print(dresponse, data)
            return False

    """通用方法:"""

    def getinfo(self, department_id=None, is_user=0) -> dict:
        """
        根据id获取部门或人员信息
        :param department_id:  部门id或者人员id
        :param is_user:  默认=0，为查找部门，部门id为空则返回全部部门信息，1为查找人员，人员id不能为空
        :return:  dict
        """
        if is_user:
            rget = requests.get(url=self.get_staff_url % (self.getToken, department_id))  # 返回Response类
        else:
            rget = requests.get(url=self.get_department_url % (self.getToken, department_id))

        dresponse = rget.json()
        print(dresponse)
        return dresponse

    def delinfo(self, department_id, is_user=0) -> bool:
        if is_user:
            operation = "人员删除"
            rget = requests.get(url=self.del_staff_url % (self.getToken, department_id))  # 返回Response类
        else:
            operation = "部门删除"
            rget = requests.get(url=self.del_department_url % (self.getToken, department_id))  # 返回Response类
        result = self.printResult(operation, department_id, rget)
        return result

    def createinfo(self, info_data: dict, is_user=0) -> bool:
        cdata = json.dumps(info_data, ensure_ascii=False).encode("utf-8")  # 将python dict转为json并以utf-8编码
        if is_user:
            operation = "人员创建"
            rpost = requests.post(url=self.create_staff_url % self.getToken, data=cdata)  # 使用post请求完成部门创建，返回Response类
        else:
            operation = "部门创建"
            rpost = requests.post(url=self.create_department_url % self.getToken, data=cdata)
        result = self.printResult(operation, info_data, rpost)  # 打印操作结果
        return result

    def updateinfo(self, new_data: dict, is_user=0) -> bool:
        cdata = json.dumps(new_data, ensure_ascii=False).encode("utf-8")  # 将python dict转为json并以utf-8编码
        if is_user:
            operation = "人员修改"
            rpost = requests.post(url=self.update_staff_url % self.getToken, data=cdata)  # 使用post请求完成部门创建，返回Response类
        else:
            operation = "部门修改"
            rpost = requests.post(url=self.update_department_url % self.getToken, data=cdata)
        result = self.printResult(operation, new_data, rpost)  # 打印操作结果
        return result

    def delAllDepartmentf(self):
        # 操作方法：
        # 在kettl中向中间表中推送空数据，则中间表中的数据config字段会被标记为3，再运行本程序，会将所有数据从微信中删除
        pass

    def upload(self, file_name):
        """使用post请求上传临时文件到企业微信，返回文件的media_id"""
        files = {'file': open(file_name, 'rb')}

        response = requests.post(self.upload_url % self.getToken, files=files)
        js = response.json()
        media_id = js['media_id']
        print("%s上传成功 media_id：%s" % (file_name, media_id))
        return media_id

    def coverAll(self, media_id, is_user=0):
        """"""
        body_dep = json.dumps({"media_id": media_id, "callback": {"url": "", "token": "", "encodingaeskey": ""}})
        body_staff = json.dumps(
            {"media_id": media_id, "to_invite": False, "callback": {"url": "", "token": "", "encodingaeskey": ""}})

        if is_user:
            job = "人员"
            response = requests.post(self.cover_staff_url % self.getToken, data=body_staff)
        else:
            job = "部门"
            response = requests.post(self.cover_deparment_url % self.getToken, data=body_dep)

        js = response.json()
        if js['errcode'] == 0:
            print("%s覆盖成功 jobid：%s" % (job, js['jobid']))
        else:
            print(js)

        return js


# 暂时作废，用于逐个处理部门
def handleDepartment(wechat, db):
    """
    @param wechat:  自定义类Wechat，用于确定企业微信身份
    @param db:  自定义类Database， 用于确定要连接的数据库

    中间表字段控制：
        config：比对数据源，新增置1，修改置2，删除置3
        status：比对数据源，有新增、修改和删除处理则置1，在对企业微信进行处理之后置0

    函数功能：
         - 在中间表中查找需要处理的部门数据
         - 遍历需要处理的数据，在企业微信中处理掉，处理后修改中间表数据 将状态置0
         - 关闭数据库连接（数据库连接在DataBase类中完成，在本函数处理结束后关闭）

    """
    query_sql_for_createupdate = """
            SELECT id,od, parentid,name,name_en, config 
            FROM WECHAT_PERSONNEL_DEPARTMENT 
            WHERE STATUS=1 order by parentid,id
    """  # 在中间表中查询 有需要变更的部门信息
    query_sql_for_del = """SELECT id FROM WECHAT_PERSONNEL_DEPARTMENT 
                            WHERE STATUS=1 AND CONFIG=3 ORDER BY parentid DESC"""  # 查询需要删除的部门信息CONFIG=3

    update_sql = """UPDATE WECHAT_PERSONNEL_DEPARTMENT SET STATUS=0 WHERE id='%s'"""  # 对中间表已处理的数据置0 STATUS=0
    delete_sql = """DELETE FROM WECHAT_PERSONNEL_DEPARTMENT WHERE id='%s'"""

    data_title = ['id', 'order', 'parentid', 'name', 'name_en']
    createupdate_date = db.query(query_sql_for_createupdate)
    del_data = db.query(query_sql_for_del)

    for department in del_data:
        id = department[0]
        success = wechat.delinfo(id)  # 删除微信中的部门
        if success:
            db.delete(delete_sql % id)  # 删除中间表数据

    for department in createupdate_date:
        data = dict(zip(data_title, department))  # 组织dict数据
        if department[-1] == 2:
            success = wechat.updateinfo(data)  # 更新部门
        elif department[-1] == 1:
            success = wechat.createinfo(data)  # 创建部门
        else:
            success = False

        if success:
            db.update(update_sql % department[0])  # 将中间表status字段置0

    db.connClose()


# 暂时作废，用于逐个处理人员
def handleStaff(wechat, db):
    query_sql_for_createupdate = """
            SELECT GH,XM,XB,LXDH,BGDH,DZXX,SZKS,OD,ZW,IS_LEADER,TO_INVITE,config 
            FROM WECHAT_PERSONNEL_TEACHER WHERE STATUS=1  AND  SZKS LIKE '402___'
    """
    query_sql_for_del = """SELECT GH FROM WECHAT_PERSONNEL_TEACHER 
                        WHERE STATUS=1 AND CONFIG=3 AND SZKS LIKE '402___' ORDER BY GH"""
    update_sql = """UPDATE WECHAT_PERSONNEL_TEACHER SET STATUS=0 WHERE GH='%s'"""
    delete_sql = """DELETE FROM WECHAT_PERSONNEL_TEACHER WHERE GH='%s'"""

    data_title = ['userid', 'name', 'gender', 'mobile', 'telephone', 'email', 'department', 'order', 'position',
                  'is_leader_in_dept', 'to_invite']
    createupdate_date = db.query(query_sql_for_createupdate)
    del_data = db.query(query_sql_for_del)

    for staff in del_data:
        id = staff[0]
        success = wechat.delinfo(id, 1)  # 删除微信中的人员
        if success:
            db.delete(delete_sql % id)  # 删除中间表数据

    for staff in createupdate_date:
        data = dict(zip(data_title, staff))  # 组织dict数据
        if data['to_invite']:
            data['to_invite'] = True  # 是否发送邀请
        else:
            data['to_invite'] = False
        if staff[-1] == 2:
            success = wechat.updateinfo(data, 1)  # 更新人员
            if not success:
                success = wechat.createinfo(data, 1)
        elif staff[-1] == 1:
            success = wechat.createinfo(data, 1)  # 创建人员

        else:
            success = False

        if success:
            db.update(update_sql % staff[0])  # 将中间表status字段置0

    db.connClose()


"""运行函数"""


@timer
def run(wechat: Wechat, db: Database):
    """部门数据"""
    dep_file_name = "department" + ".csv"
    dep_list = ['部门名称', '部门ID', '父部门ID', '排序']
    dep_data = db.query(
        """SELECT DWMC NAME,DWH ID, (CASE LSDWH WHEN '0' THEN '1' ELSE LSDWH END) parentid, 999999-DWH OD 
           FROM V_DLAKE_DEPARTMENT_SIMP WHERE  DWYXBS ='0' AND DWH<>'0' order by dwh """)  # get data from db
    toCSV(dep_data, dep_list, dep_file_name)  # 输出到CSV
    department_media_id = wechat.upload(dep_file_name)  # upload data and get media_id=
    wechat.coverAll(department_media_id)  # cover all

    """教师数据"""
    tch_file_name = "teacher" + ".csv"
    tch_list = ["姓名", "帐号", "手机号", "邮箱", "所在部门", "职位", "性别", "是否部门内领导", "排序", "别名", "地址", "座机", "禁用",
                "禁用项说明：(0-启用;1-禁用)"]
    tch_data = db.query("""SELECT XM,GH,LXDH,DZXX,SZKS_SIMP,ZW,XBM, (CASE WHEN XH='01' THEN 1 ELSE 0 END) IS_LEADER,
                                9999999-XH OD,'' BM,'' DZ,BGDH,0 JY, '' SM FROM V_DLAKE_TEACHER_SIMP 
                            WHERE GZZT='在职' AND SUBSTR(GH,0,4)<> '9999' 
                             AND JZGLBM  NOT IN ('兼职教师') AND (SZKS_SIMP LIKE '402%' ) """)
    toCSV(tch_data, tch_list, tch_file_name)  # 输出到CSV
    teacher_media_id = wechat.upload(tch_file_name)
    wechat.coverAll(teacher_media_id, is_user=1)


if __name__ == '__main__':
    jisu = Wechat()
    dbm = Database('oracle', 'dbm/comsys123@dbm')

    # handleDepartment(jisu, dbm)
    # handleStaff(jisu, dbm)
    #
    # data = {
    #     "userid": "2009800023",
    #     "name": "王尊",
    #     "mobile": "18686669798",
    #     "department": "316999",
    #     "order": "11",
    #     "position": "职员",
    #     "gender": "1",
    #     "is_leader_in_dept": 0,
    #     "to_invite": False  # 微信邀请
    # }
    #
    # res = jisu.createinfo(data, 1)
    # print(res)

    run(jisu, dbm)
