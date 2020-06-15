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
ob.dictionary_writer(suspicious_shop_users, "out/", "python_output.csv")
