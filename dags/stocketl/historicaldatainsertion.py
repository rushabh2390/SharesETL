import asyncio
import os
import pathlib
import csv
from datetime import datetime
import psycopg2
import os
from database import Database

async def main():
    allowed_file = [".csv"]
    path = "dumpdata"
    if not os.path.exists(path):
        print("no path found")
        return
    for fileName in os.listdir(path):
        # check if current path is a file
        if os.path.isfile(os.path.join(path, fileName)):
            file = pathlib.Path(os.path.join(path, fileName))
            if file.suffix in allowed_file:
                con = Database.getConnection()
                cur = con.cursor()
                with open(file, 'r') as fin:
                    dr = csv.DictReader(fin)
                    for i in dr:
                        print("printing iteration =========>>>>>>>>>", i)
                        ondate = datetime.strptime(
                            i['ondate'], "%d-%m-%Y").strftime("%Y-%m-%d")
                        stock_info = (i['stock_id'], ondate, float(i['openat']), float(i['high']),
                                      float(i['low']), float(i['closeat']), float(i['volume']), float(i['hl']), float(i['oc']))
                        print("printing ondate ==========>>>>>>>", ondate)
                        print("printing stock_info =================>>>>>>>>", stock_info)
                        cur.execute("insert into stock_data_history (stock_code,ondate,openat,high,low,closeat,volume,hl,oc) values(%s,%s,%s,%s,%s,%s,%s,%s,%s);", stock_info)
                    con.commit()
                cur.close()
                os.remove(file)

    cur = con.cursor()
    cur.execute(
        "SELECT stock_code, max(ondate) FROM stock_data_history group by stock_code")
    rows = cur.fetchall()
    for row in rows:
        cur.execute("update stock_details set stock_value_latest_date=%s where id=%s", [
                    row[1], row[0]])
    con.commit()
    cur.close()
asyncio.run(main())
