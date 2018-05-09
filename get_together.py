# coding=utf-8
#utf-8
import json
import os
import pymysql
import time

MAX_THREAD_EACH_GROUP=20
LOG_FILE="./threads_to_kill.log"
EXECUTE_KILL=False

conn = pymysql.connect(host='127.0.0.1', port=3306, user='root', passwd='', db='clusterThreadCount', charset='utf8')
cursor = conn.cursor()



#根据输入的thread信息去不同的机器上kill进程
def kill_thread(thread_info):
    machine_num,pid=thread_info.split("_")
    kill_command="kill %d"%pid
    shell_command="ssh 172.31.32.%d '%s'"%(machine_num,kill_command)
    ssh_result=os.popen(shell_command)
    print(ssh_result.read())


for i in range(10):
    cur_machine = i + 1
    file = "./icst" + str(cur_machine) + ".json"
    if os.path.exists(file):
        with open(file, "r") as f:
            gpu_info_each_server = json.load(f)
            for gpu_id, gpu_info in gpu_info_each_server.items():
                temp_count = {}  # 处理一个GPU上有用户多个进程的问题，一个GPU上用户有多个进程依然只计算一个
                for process in gpu_info["processes"]:
                    if process != None:
                        temp_username = process["username"]
                        temp_pid = str(cur_machine) + "_" + str(process["pid"])
                        temp_group = os.popen("groups %s" % temp_username).read().split(":")[1].strip()
                        temp_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                        try:
                            temp_sql="INSERT INTO current_thread(pid,user_name,group_name,submit_time) VALUE ('%s','%s','%s','%s');" % (
                                temp_pid, temp_username, temp_group, temp_time)
                            # print(temp_sql)
                            effect_row = cursor.execute(temp_sql)
                            # 提交，不然无法保存新建或者修改的数据
                            conn.commit()
                            if effect_row != 1:
                                print("database insert failed")
                        except:
                            print("database insert exception")
    else:
        print(file, "is not existed")



sql1 = "DELETE FROM previous_thread WHERE pid NOT IN (SELECT  pid FROM  current_thread);"
try:
    effect_row=cursor.execute(sql1)   #删除掉previous_thread数据表里已经结束的进程信息
    conn.commit()
    print("%d rows in previous_thread have been delete"%effect_row)
except:
    print("database delete exception in previous_thread")



sql2= "DELETE FROM current_thread WHERE pid  IN (SELECT  pid FROM  previous_thread);"
try:
    effect_row=cursor.execute(sql2)   #删除掉current_thread中之前已经在运行的进程
    conn.commit()
    print("%d rows in current_thread have been delete" % effect_row)
except:
    print("database delete exception in current_thread")


# 获得当前存在新的线程的组
sql3="SELECT group_name FROM current_thread GROUP BY group_name;"
new_thread_groups=[]
try:
    cursor.execute(sql3)  # 执行sql语句
    query_result = cursor.fetchall()  # 获取查询的所有记录
    for rows in query_result:
        new_thread_groups.append(rows[0])
except:
    print("database query exception in current_thread")


# 把所有的新的线程插入到previous_thread数据表
sql4="INSERT INTO previous_thread SELECT * FROM current_thread;"
try:
    effect_row=cursor.execute(sql4)   #将current_thread中的所有进程都插入到previous_thread里
    conn.commit()
    print("%d rows  have been added into previous_thread" % effect_row)
except:
    print("database insert exception in previous_thread")


#判断新插入线程的组 有没有超出限制
threads_need_to_be_killed=[]
for group_name in new_thread_groups:
    thread_count_sql="SELECT COUNT(*) FROM previous_thread WHERE group_name='%s';" % group_name
    try:
        cursor.execute(thread_count_sql)
        query_result=cursor.fetchall()
        cur_group_thread_count=query_result[0][0]
        if cur_group_thread_count>MAX_THREAD_EACH_GROUP:
            num_need_to_kill=cur_group_thread_count-MAX_THREAD_EACH_GROUP
            kill_sql="SELECT * FROM previous_thread WHERE group_name='%s' ORDER BY submit_time DESC LIMIT %d ;"%(group_name,num_need_to_kill)
            cursor.execute(kill_sql)
            query_result=cursor.fetchall()
            for rows in query_result:
                threads_need_to_be_killed.append(rows)
                temp_delete_sql="DELETE FROM previous_thread WHERE pid='%s'"%rows[1]
                effect_row=cursor.execute(temp_delete_sql)
                conn.commit()
    except:
        print("database exception in previous_thread when get the need killed thread")


empty_current_tread_sql="DELETE FROM current_thread;"
try:
    effect_row=cursor.execute(empty_current_tread_sql)
    conn.commit()
    print("current_thread table has been emptied with %d rows have been deleted"%effect_row)
except:
    print("database exception in previous_thread when empty the table")


conn.commit()
# 关闭游标
cursor.close()
# 关闭连接
conn.close()

if not os.path.exists(LOG_FILE):
    temp=open(LOG_FILE,"w")
    temp.close()

with open(LOG_FILE,"a+") as fp:
    for rows in threads_need_to_be_killed:
        print(rows)
        if EXECUTE_KILL:   #如果需要结束进程的话
            kill_thread(rows[1])

        fp.writelines(str(rows)+" @@ "+time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())+"\n")





