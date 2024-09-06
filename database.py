"""
Database File
Copyright 2023-2024 tohid.jk
License GNU GPLv2 or later
2024-09-06
"""


import os
import atexit
import typing
import io

import json
import csv

import lzma as compression # lzma,bz2,gzip,zlib
import crypt as encryption


class Database(typing.Dict):
    def __init__(self,
        fileName: str,
        compress: bool = False,
        password: str = None,
        saveAtExit: bool = True
    ):
        self.fileName = fileName
        self.compress = compress
        self.password = password
        self.saveAtExit = saveAtExit

        if os.path.isfile(self.fileName):
            self.read()

        if self.saveAtExit:
            atexit.register(self.write)

    def readfile(self) -> str:
        data = None
        with open(self.fileName, "rb") as file:
            data = file.read()

        if self.password != None:
            data = encryption.decrypt(data, self.password)

        if self.compress:
            data = compression.decompress(data)

        return data.decode()

    def writefile(self, data: str) -> None:
        data = data.encode()

        if self.compress:
            data = compression.compress(data)

        if self.password != None:
            data = encryption.encrypt(data, self.password)

        with open(self.fileName, "wb") as file:
            file.write(data)

    def read(self) -> None:
        self.clear()
        self.update(eval(self.readfile()))

    def write(self) -> None:
        self.writefile(self.__str__())

    def commit(self) -> None:
        self.write()

class DatabaseJSON(Database):
    def read(self) -> None:
        self.clear()
        self.update(json.loads(self.readfile()))

    def write(self) -> None:
        self.writefile(json.dumps(self, indent="\t"))

class DatabaseCSV(Database):
    def read(self) -> None:
        buffer = io.StringIO(self.readfile())
        csvreader = csv.reader(buffer)
        self.headers = next(csvreader, list())
        self.data = list(csvreader)

    def write(self) -> None:
        buffer = io.StringIO()
        csvwriter = csv.writer(buffer, lineterminator="\n")
        csvwriter.writerow(self.headers)
        csvwriter.writerows(self.data)
        self.writefile(buffer.getvalue())

    @property
    def headers(self) -> list:
        return self.get("headers")

    @headers.setter
    def headers(self, value: list) -> None:
        self.update({"headers" : value})

    @property
    def data(self) -> list:
        return self.get("data")

    @data.setter
    def data(self, value: list) -> None:
        self.update({"data" : value})

class DatabaseFolder:
    def __init__(self,
        folderName: str,
        databaseType: typing.Type[Database] = DatabaseJSON,
        compress: bool = False,
        password: str = None,
        saveAtExit: bool = True
    ):
        self.folderName = folderName
        self.databaseType = databaseType
        self.compress = compress
        self.password = password
        self.saveAtExit = saveAtExit

        if not os.path.isdir(self.folderName):
            os.mkdir(self.folderName)

    def database(self, fileName: str) -> typing.Type[Database]:
        return self.databaseType(
            os.path.join(self.folderName, fileName),
            self.compress,
            self.password,
            self.saveAtExit
        )


if __name__ == "__main__":
    compress, password = False, None

    h = ["firstname", "lastname", "age"]
    d = [[f"fn{i}", f"ln{i}", i] for i in range(1000)]

    db = Database("database", compress, password)
    db["headers"] = h
    db["data"] = d
    db.commit()

    dbjson = DatabaseJSON("databasejson", compress, password)
    dbjson["headers"] = h
    dbjson["data"] = d
    dbjson.commit()

    dbcsv = DatabaseCSV("databasecsv", compress, password)
    dbcsv.headers = h
    dbcsv.data = d
    dbcsv.commit()

    dbfolder = DatabaseFolder("datafolder", DatabaseJSON, compress, password)
    f1 = dbfolder.database("file1")
    f1["headers"] = h
    f1["data"] = d
    f1.commit()

