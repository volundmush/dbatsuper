#! /usr/bin/env python3
import sys
from pathlib import Path
import os
import orjson
import psycopg2
import time

json = orjson


def readJsonFile(dump_dir: Path, file_name: str):
    with open(dump_dir / file_name) as f:
        data = json.loads(f.read())
    return data

def tool_index_obj_apply(dump_dir: Path, args):
    location = int(args[0])

    data = readJsonFile(dump_dir, "itemPrototypes.json")

    print(f"Searching through {len(data)} prototypes for affected: {location}")
    total = 0
    for proto in data:
        affect = []

        for aff in proto.get("affected", list()):
            if (loc := aff.pop("location", -1)) == location:
                affect.append(aff)

        if affect:
            total += 1
            print(f"VN: {proto['vn']} - {proto['name']} - {len(affect)} affects found")
            for aff in affect:
                print(repr(aff))

    print(f"Total Found: {total}")


def tool_index_obj_flag(dump_dir: Path, args):
    flag = int(args[0])

    data = readJsonFile(dump_dir, "itemPrototypes.json")

    print(f"Searching through {len(data)} prototypes for flag: {flag}")
    total = 0
    for proto in data:

        if flag in set(proto.get("extra_flags", list())):
            total += 1
            print(f"VN: {proto['vn']} - {proto['name']}")

    print(f"Total Found: {total}")

def tool_load_sql(dump_dir: Path, args):
    sql_data = readJsonFile(Path.cwd(), "dbconf.json")
    conn = psycopg2.connect(**sql_data)
    
    cur = conn.cursor()
    
    
    
    rooms = set()
    
    data_to_insert = list()

    for j in readJsonFile(dump_dir, "rooms.json"):
        rooms.add(j.get("id"))
        data_to_insert.append((j.get("id"), j.get("name")))
    
    sql = """INSERT INTO dbat.rooms (id, name) VALUES (%s, %s)"""
    cur.executemany(sql, data_to_insert)
    
    data_to_insert.clear()
    for j in readJsonFile(dump_dir, "exits.json"):
        room = j.get("room")
        if room not in rooms:
            continue
        direction = j.get("direction")
        if (data := j.get("data", dict())):
            if (dest := data.get("to_room")):
                if dest not in rooms:
                    continue
                data_to_insert.append((room, direction, dest))
    
    sql = """INSERT INTO dbat.exits (room, direction, destination) VALUES (%s, %s, %s)"""
    cur.executemany(sql, data_to_insert)
    
    conn.commit()
    
    cur.close()
    
    conn.close()

def tool_load_all(dump_path: Path, *args):
    data = dict()
    start_time = time.time()
    for file in dump_path.iterdir():
        if file.is_file():
            data[file.stem] = readJsonFile(dump_path, file.name)
    end_time = time.time()
    print(f"Loaded {len(data)} files in {end_time - start_time} seconds.")
    # wait for console input.
    input("Press Enter to continue...")

tools = {
    # Provided an affect (and, optionally, a specific), lists everything with matching obj_affected_type
    "SearchObjApply": tool_index_obj_apply,
    "SearchObjFlag": tool_index_obj_flag,
    "LoadSQL": tool_load_sql,
    "LoadItAll": tool_load_all
}

def main():
    # Read the dump directory from command line arguments or use default
    dump_directory = Path.cwd() / "dump"

    try:
        if len(sys.argv) < 2:
            raise ValueError("Not enough arguments to select operation.")
        if not (func := tools.get(sys.argv[1], None)):
            raise ValueError(f"{sys.argv[1]}: Not a valid operation.")
        print("Running operation...")
        func(dump_directory, sys.argv[2:])
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()