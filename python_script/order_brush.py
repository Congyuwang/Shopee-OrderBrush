#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jun 14 09:25:17 2020

@author: congyuwang
"""

# %% import packages and data
import pandas as pd
import numpy as np
import os
from orderbrush import Orderbrush as ob

# %% define a CSV writer


def dictionary_writer(shop_users: dict, path: str, filename: str):
    """
    pass a dictionary, write the data into CSV as required.
    """
    if not os.path.exists(path):
        os.makedirs(path)

    with open(path + filename, "w") as f:
        f.write("shopid,userid\n")
        for shopId, users in shop_users.items():
            f.write(str(shopId) + ",")
            if users is None:
                f.write("0")
            else:
                count = 0
                for user in users:
                    if count != 0:
                        f.write("&")
                    f.write(str(user))
                    count += 1
            f.write("\n")


# %% import data
df = pd.read_csv("data/order_brush_order.csv")

# %% dateTime to int
df["event_time"] = pd.to_datetime(df["event_time"]).astype(np.int64) / 10 ** 9

# %% convert to numpy array
data = df.to_numpy(dtype=np.int64)
df = None

# %% feed data into processor
orderBrush: ob = ob(data)

# %% get suspicious shop users
suspicious_shop_users: dict = orderBrush.get_suspicious_shop_users()

# %% write the result
dictionary_writer(suspicious_shop_users, "out/", "python_output.csv")
