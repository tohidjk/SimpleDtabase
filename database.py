"""
Database File
Copyright 2023-2024 tohid.jk
License GNU GPLv2 or later
2024-09-19
"""


import os
import atexit
import typing
import io

import json
import csv

import crypt as encryption
import lzma, bz2, gzip, zlib # compression


class Database(typing.Dict):
    def __init__(self,
        fileName: str,
        password: str = None,
        compression = None,
        saveAtExit: bool = True
    ):
        self.fileName = fileName
        self.password = password
        self.compression = compression
        self.saveAtExit = saveAtExit

        if os.path.isfile(self.fileName):
            self.read()

        if self.saveAtExit:
            atexit.register(self.write)

    def readFile(self) -> str:
        data = None
        with open(self.fileName, "rb") as file:
            data = file.read()

        if self.password != None:
            data = encryption.decrypt(data, self.password)

        if self.compression != None:
            data = self.compression.decompress(data)

        return data.decode()

    def writeFile(self, data: str) -> None:
        data = data.encode()

        if self.compression != None:
            data = self.compression.compress(data)

        if self.password != None:
            data = encryption.encrypt(data, self.password)

        with open(self.fileName, "wb") as file:
            file.write(data)

    def read(self) -> None:
        self.clear()
        self.update(eval(self.readFile()))

    def write(self) -> None:
        self.writeFile(self.__str__())

    def commit(self) -> None:
        self.write()

class DatabaseJSON(Database):
    def read(self) -> None:
        self.clear()
        self.update(json.loads(self.readFile()))

    def write(self) -> None:
        self.writeFile(json.dumps(self, indent="\t"))

class DatabaseCSV(Database):
    def read(self) -> None:
        buffer = io.StringIO(self.readFile())
        csvReader = csv.reader(buffer)
        self.headers = next(csvReader, list())
        self.data = list(csvReader)

    def write(self) -> None:
        buffer = io.StringIO()
        csvWriter = csv.writer(buffer, lineterminator="\n")
        csvWriter.writerow(self.headers)
        csvWriter.writerows(self.data)
        self.writeFile(buffer.getvalue())

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
        password: str = None,
        compression = None,
        saveAtExit: bool = True,
        dbType: typing.Type[Database] = DatabaseJSON
    ):
        self.folderName = folderName
        self.password = password
        self.compression = compression
        self.saveAtExit = saveAtExit
        self.dbType = dbType

        if not os.path.isdir(self.folderName):
            os.mkdir(self.folderName)

    def database(self, fileName: str) -> typing.Type[Database]:
        return self.dbType(
            os.path.join(self.folderName, fileName),
            self.password,
            self.compression,
            self.saveAtExit
        )


if __name__ == "__main__":
    p, c = None, None
    h = ["firstname", "lastname", "age"]
    d = [[f"fn{i}", f"ln{i}", i] for i in range(1000)]

    db = Database("database", p, c)
    db["headers"] = h
    db["data"] = d
    db.commit()

    dbjson = DatabaseJSON("databasejson", p, c)
    dbjson["headers"] = h
    dbjson["data"] = d
    dbjson.commit()

    dbcsv = DatabaseCSV("databasecsv", p, c)
    dbcsv.headers = h
    dbcsv.data = d
    dbcsv.commit()

    dbfolder = DatabaseFolder("databasefolder", p, c)
    db1 = dbfolder.database("database1")
    db1["headers"] = h
    db1["data"] = d
    db1.commit()

