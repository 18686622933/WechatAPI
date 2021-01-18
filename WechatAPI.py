#!/usr/bin/env python3
# -*- coding:utf-8 -*-


import requests
import json
import cx_Oracle
import pymysql
import pymssql
import sqlite3
import datetime
import time

# test data
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

    def updata2select(self, updata_sql) -> tuple:
        """将uptdata语句转换为select语句，用于修改后的查询"""
        words = updata_sql.split()
        upper_words = list(map(lambda x: x.upper(), words))

        if 'UPDATE' in upper_words and 'SET' in upper_words and 'WHERE' in upper_words:
            table = words[upper_words.index('UPDATE') + 1]
            conditional = ' '.join(words[upper_words.index('WHERE') + 1:])
            set = words[upper_words.index('SET') + 1]
            key = set.split('=')[0]
            value = set.split('=')[1].replace('\'', '')

        else:
            return None

        select_sql = "SELECT %s FROM %s WHERE %s" % (key, table, conditional)
        return select_sql, value


class Wechat:
    def __init__(self):
        self.corpid = "wwfdfdbe459f6eded7"  # 企业id
        self.corpsecret = "mjF5UDXrO5dbdK3kT0Tm2_OGxg4HbXZjFai5ukYfHOE"  # 通讯录secret
        self.token_url = "https://qyapi.weixin.qq.com/cgi-bin/gettoken?corpid=%s&corpsecret=%s"  # 获取token的地址，需要get请求
        self.token_response = requests.get(url=self.token_url % (self.corpid, self.corpsecret))
        self.getToken = self.token_response.json().get('access_token')
        if self.getToken:
            print("token获取成功")
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

    """公用函数"""

    def pp(self, data: dict):
        """pretty print 格式化打印json"""
        ppdata = json.dumps(data, indent=4, separators=(',', ':'), ensure_ascii=False)
        print(ppdata)
        return ppdata

    def printResult(self, operation, data, response):
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

    def delinfo(self, department_id, is_user=0):
        if is_user:
            operation = "人员删除"
            rget = requests.get(url=self.del_staff_url % (self.getToken, department_id))  # 返回Response类
        else:
            operation = "部门删除"
            rget = requests.get(url=self.del_department_url % (self.getToken, department_id))  # 返回Response类
        result = self.printResult(operation, department_id, rget)
        return result

    def createinfo(self, info_data: dict, is_user=0):
        cdata = json.dumps(info_data, ensure_ascii=False).encode("utf-8")  # 将python dict转为json并以utf-8编码
        if is_user:
            operation = "人员创建"
            rpost = requests.post(url=self.create_staff_url % self.getToken, data=cdata)  # 使用post请求完成部门创建，返回Response类
        else:
            operation = "部门创建"
            rpost = requests.post(url=self.create_department_url % self.getToken, data=cdata)
        result = self.printResult(operation, info_data, rpost)  # 打印操作结果
        return result

    def updateinfo(self, new_data: dict, is_user):
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

    """人员方法:"""


@timer
def handleDepartment(wechat, db):
    query_sql_for_createupdate = """
            SELECT DWH id,999999-DWH od, (CASE LSDWH WHEN '0' THEN '1' ELSE LSDWH END) parentid,DWMC name,DWJP name_en, config 
            FROM WECHAT_PERSONNEL_DEPARTMENT 
            WHERE STATUS=1 AND DWH<>0 order by LSDWH,DWH
    """
    query_sql_for_del = """SELECT DWH FROM WECHAT_PERSONNEL_DEPARTMENT WHERE STATUS=1 AND CONFIG=3 ORDER BY LSDWH DESC"""
    update_sql = """UPDATE WECHAT_PERSONNEL_DEPARTMENT SET STATUS=0 WHERE DWH='%s'"""
    delete_sql = """DELETE FROM WECHAT_PERSONNEL_DEPARTMENT WHERE DWH='%s'"""

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


if __name__ == '__main__':
    jisu = Wechat()
    # dbm = Database('oracle', 'dbm/comsys123@dbm')

    # handleDepartment(jisu, dbm)
    # jisu.getinfo('chenbowen', 1)
    # jisu.getinfo()
    # jisu.delinfo('2010800037', 1)

    data = {
        "userid": '2010800037',
        "name": '杨慧',
        "mobile": '15567778998',
        "department": 1,  # 部门
        "position": "",  # 职务
        "gender": "2",  # 性别
        # "is_leader_in_dept":0,  # 是否为部门领导
        "to_invite": False  # 是否发送邀请
    }

    # jisu.createStaff(data)
    jisu.createinfo(data, 1)
    jisu.getinfo('2010800037', 1)

# kettle中间表
# handlestaff
# log
