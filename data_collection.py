import json
from string import ascii_uppercase as uc
import gspread
from oauth2client.service_account import  ServiceAccountCredentials
from pyquery import PyQuery as pq
from time import sleep
from collections import defaultdict
import math
from time import sleep
import csv
import yaml
import requests

config_yaml = "./conf/config.yaml"
scope = ['https://spreadsheets.google.com/feeds']
f = open(config_yaml, 'r')
conf = yaml.load(f)
f.close()
credentials = ServiceAccountCredentials.from_json_keyfile_name(conf["jsonkey"], scope)
gc = gspread.authorize(credentials)
sheet = gc.open(conf["google_sheet_title"])
# データソースごとにwksの名前をつける。
# wks = sheet.worksheet(conf["wks_num"])


def main():
    records = content_as_json(conf["data_sheet"], conf["data_sheet_num"])
    # 処理はconfに設定したrowsのデータごと走らせる。
    for r in conf["rows"]:
        row_id = r["wks_id"]
        row_name = r["name"]
        info = list(filter(lambda x: x["ID"] == row_id, records))
        # タイプに応じたパーサーを呼び出し、データをスプレッドシートに一時保存する
        globals()[r["method"]](row_id, row_name,  info)


def get_csv(i, n, lst):
    obj = lst[0]
    #names = obj.keys()
    url = obj["データソース"]
    with requests.Session() as s:
        res = s.get(url)

    res = res.content.decode('utf-8')

    #cr = csv.reader(res.splitlines(), delimiter=',')
    #row_info = list(cr)
    #res = res.content.decode('utf-8')
    #info_lst = [x for x in res]
    #lst2sheet(n, row_info)

    #reader = csv.reader(decoded_content.splitlines(), delimiter=',')
    #lst = list(reader)
    lst2sheet(n, res)

    # CSVの場合、データソースに記述されたcsvを取得し、そのまま
    # スプレッドシートに一時保存する
    #json2sheet(names, l)


def get_html(i, n, l):
    pass


def content_as_json(title, ws):
    wks_num = ws
    scope = ['https://spreadsheets.google.com/feeds/']
    data_credentials = ServiceAccountCredentials.from_json_keyfile_name(conf['jsonkey'], scope)
    gc = gspread.authorize(data_credentials)
    sht = gc.open(title)
    wks = sht.get_worksheet(wks_num)
    l = wks.get_all_records()
    return l


def json2sheet(name, dct):
    # rowsのwksの名前
    wks = sheet.worksheet(conf["wks_num"])
    block = 'A2:{}{}'.format(uc[len(name) - 1], len(dct) + 1)
    cell_list = wks.range(block)

    # dictをflattenする
    vals = []
    for o in dct:
        for n in name:
            vals.append(o[n])

    # flattenしたdictとsheetのrangeリストをzip()する
    i = 0
    for cell, val in zip(cell_list, vals):
        cell.value = val

    # sheetを更新する
    wks.update_cells(cell_list)


def lst2sheet(name, c):
    print(c)
    scope = [
        'https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/drive'
    ]
    credentials = ServiceAccountCredentials.from_json_keyfile_name(conf["jsonkey"], scope)
    gc = gspread.authorize(credentials)
    sheet = gc.open(conf["google_sheet_title"])
    gc.import_csv(sheet.id, c)


main()
