import re
import csv
import json

import html
import requests

from urllib.parse import urlparse

def mk_options(options):
    res = {}
    for opt in options:
        _k = opt['name']
        _v = opt['values']
        if res.get(_k, ''):
            res[_k] = list(set(res[_k] + _v))
        else:
            res[_k] = _v
    return res

def mk_tags(opt):
    return ','.join(set(opt))

def mk_images(opt):
    ll = sorted(opt, key=lambda x: x['position'])
    rl = []
    rs = set()
    for l in ll:
        if l['src'] not in rs:
            rs.add(l['src'])
            rl.append(l['src'].replace("comdata", "com/data"))
    return rl

def make_df(opts, images):
    Opt1Name, Opt2Name, Opt3Name = '', '', ''
    Opt1Val, Opt2Val, Opt3Val = [], [], []
    if len(opts) > 3:
        raise Exception(f"len opts is to long: {len(opts)}")
    elif len(opts) == 3:
        Opt1Name, Opt2Name, Opt3Name = opts[0]["name"], opts[1]["name"], opts[2]["name"]
        Opt1Val, Opt2Val, Opt3Val = opts[0]["values"], opts[1]["values"], opts[2]["values"]
    elif len(opts) == 2:
        Opt1Name, Opt2Name = opts[0]["name"], opts[1]["name"]
        Opt1Val, Opt2Val = opts[0]["values"], opts[1]["values"]
    elif len(opts) == 1:
        Opt1Name = opts[0]["name"]
        Opt1Val = opts[0]["values"]

    df = []
    for _v1 in range(len(Opt1Val)):
        if Opt2Val:
            for _v2 in range(len(Opt2Val)):
                if Opt3Val:
                    for _v3 in range(len(Opt3Val)):
                        row = [""] * 49
                        row[7], row[8] = Opt1Name, Opt1Val[_v1]
                        row[9], row[10] = Opt2Name, Opt2Val[_v2]
                        row[11], row[12] = Opt3Name, Opt3Val[_v3]
                        df.append(row)
                else:
                    row = [""] * 49
                    row[7], row[8] = Opt1Name, Opt1Val[_v1]
                    row[9], row[10] = Opt2Name, Opt2Val[_v2]
                    df.append(row)
        else:
            row = [""] * 49
            row[7], row[8] = Opt1Name, Opt1Val[_v1]
            df.append(row)
    line = len(df)
    for _image in range(len(images)):
        if line > _image:
            df[_image][24] = images[_image]
            df[_image][25] = str(_image + 1)
        else:
            row = [""] * 49
            row[24] = images[_image]
            row[25] = str(_image + 1)
            df.append(row)
    return df


import pymysql

def read_row():
    conn = pymysql.connect(
        host='localhost',
        port=3306,
        user='root',
        password='12345678',
        database='mydb',
        charset="utf8mb4"
    )
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT 
            id, handle, title, description, category, original_price, images, options
        FROM
            product_info;
        """
    )

    res = cursor.fetchall()
    cursor.close()
    conn.close()

    return res


def main():

    csv_data = {}
    rows = read_row()
    for row in rows:
        csv_data[row[1]] = {
            "options": json.loads(row[7]),
            "tags": json.loads(row[4]),
            "images": json.loads(row[6]),
            "slug": row[1],
            "title": row[2],
            "price": row[5],
            "body_html": row[3],
        }


    print()
    print(f"共采集到 {len(csv_data)} 个产品")
    Vendor = 'Lucyinthesky'
    with open(f"{Vendor}.csv", "w", encoding="utf-8-sig") as f:
        f.write(
            "Handle,Title,Body (HTML),Vendor,Type,Tags,Published,Option1 Name,Option1 Value,Option2 Name,Option2 Value,Option3 Name,Option3 Value,Variant SKU,Variant Grams,Variant Inventory Tracker,Variant Inventory Qty,Variant Inventory Policy,Variant Fulfillment Service,Variant Price,Variant Compare At Price,Variant Requires Shipping,Variant Taxable,Variant Barcode,Image Src,Image Position,Image Alt Text,Gift Card,SEO Title,SEO Description,Google Shopping / Google Product Category,Google Shopping / Gender,Google Shopping / Age Group,Google Shopping / MPN,Google Shopping / AdWords Grouping,Google Shopping / AdWords Labels,Google Shopping / Condition,Google Shopping / Custom Product,Google Shopping / Custom Label 0,Google Shopping / Custom Label 1,Google Shopping / Custom Label 2,Google Shopping / Custom Label 3,Google Shopping / Custom Label 4,Variant Image,Variant Weight Unit,Variant Tax Code,Cost per item,Status,Collection\n"
        )
    for _k, _v in csv_data.items():
        opts = _v["options"]
        tag = mk_tags(_v["tags"])
        images = mk_images(_v["images"])
        Handle = _v["slug"]
        title = _v["title"]
        price = _v["price"]
        df = make_df(opts, images)
        try:
            description = _v["body_html"].replace("”", "\"").replace("’", "'").replace("&amp;", "&").replace(" ", " ")
        except:
            description = ''
        df[0][1], df[0][2], df[0][3] = title, description, Vendor
        df[0][5], df[0][6], df[0][47] = tag, "TRUE", "active"
        df[0][48] = tag
        for _row in df:
            _row[0], _row[15], _row[16], _row[17] = Handle, "shopify", "1000", "continue",
            _row[18], _row[19], _row[44] = "manual", str(price), "Kg"
        with open(f"{Vendor}.csv", "a", encoding="utf-8-sig", newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerows(df)

if __name__ == '__main__':
    main()
