#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import pickle
import pathlib


BASE_DIR = pathlib.Path(__file__).parent

config = json.load(open(BASE_DIR.joinpath("config.json"), encoding="utf-8"))
data_path = BASE_DIR.joinpath("data")
output_path = BASE_DIR.joinpath("output")

def main():
    filtered_index = json.load(open(data_path.joinpath("cn_index_filtered.json"), "r", encoding="utf-8"))

    index_dir = data_path.joinpath("index_info")


    for index in filtered_index:
        with open(index_dir.joinpath(f"{index['stockCode']}.pickle"), "rb") as f:
            index_info = pickle.load(f)



    black_list = config["black_list"]
    config["black_list"] = list(set(black_list ))

    json.dump(config, open(BASE_DIR.joinpath("config.json"), "w", encoding="utf-8"), ensure_ascii=False, indent=4)


if __name__ == '__main__':
    main()
