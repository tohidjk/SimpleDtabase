"""
Database File
Copyright 2023-2024 tohid.jk
License GNU GPLv3
2024-11-12
"""

import os
import io
import zipfile

# Database File Type
import xmltodict
import json
import yaml
import csv

# fileCompressionClass
import lzma
import bz2
import gzip
import zlib

import crypt as encryption

class Database(dict):
    def __init__(self,
        file, # filename string or read+write bytes stream
        filePassword: bytes = None,
        fileCompressionClass = None,
        saveAtExit: bool = True
    ):
        self.file = file
        self.filePassword = filePassword
        self.fileCompressionClass = fileCompressionClass
        self.saveAtExit = saveAtExit

        # filename string -> read+write bytes stream
        if isinstance(self.file, str):
            self.file = open(self.file, "a+b")

        # check file size
        self.file.seek(0, os.SEEK_END)
        if self.file.tell() > 0:
            self.read()

    def __del__(self):
        if self.saveAtExit:
            self.commit()

    def readFile(self) -> str:
        self.file.seek(0)
        data = self.file.read()

        if self.filePassword != None:
            data = encryption.decrypt(data, self.filePassword)

        if self.fileCompressionClass != None:
            data = self.fileCompressionClass.decompress(data)

        return data.decode()

    def writeFile(self, data: str) -> None:
        data = data.encode()

        if self.fileCompressionClass != None:
            data = self.fileCompressionClass.compress(data)

        if self.filePassword != None:
            data = encryption.encrypt(data, self.filePassword)

        self.file.truncate(0)
        self.file.write(data)

    def read(self) -> None:
        self.clear()
        self.update(eval(self.readFile()))

    def write(self) -> None:
        self.writeFile(str(self))

    def commit(self) -> None:
        self.write()

class DatabaseXML(Database):
    rootTag = "root"

    def read(self) -> None:
        self.clear()
        self.update(xmltodict.parse(self.readFile())[self.rootTag])

    def write(self) -> None:
        self.writeFile(xmltodict.unparse(
            {self.rootTag : self}, pretty=True, short_empty_elements=True
        ))

class DatabaseJSON(Database):
    def read(self) -> None:
        self.clear()
        self.update(json.loads(self.readFile()))

    def write(self) -> None:
        self.writeFile(json.dumps(self, indent="\t"))

class DatabaseYAML(Database):
    def read(self) -> None:
        self.clear()
        stream = io.StringIO(self.readFile())
        self.update(yaml.safe_load(stream))

    def write(self) -> None:
        stream = io.StringIO()
        yaml.dump(dict(self), stream)
        self.writeFile(stream.getvalue())

class DatabaseCSV(Database):
    def read(self) -> None:
        self.clear()
        stream = io.StringIO(self.readFile())
        csvReader = csv.reader(stream)
        self.headers = next(csvReader, list())
        self.data = list(csvReader)

    def write(self) -> None:
        stream = io.StringIO()
        csvWriter = csv.writer(stream, lineterminator="\n")
        csvWriter.writerow(self.headers)
        csvWriter.writerows(self.data)
        self.writeFile(stream.getvalue())

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

class DatabaseFolder(dict):
    def __init__(self,
        folderName: str,
        filePassword: bytes = None,
        fileCompressionClass = None,
        saveAtExit: bool = True,
        dbClass: Database = DatabaseJSON
    ):
        self.folderName = folderName
        self.filePassword = filePassword
        self.fileCompressionClass = fileCompressionClass
        self.saveAtExit = saveAtExit
        self.dbClass = dbClass

        if os.path.isdir(self.folderName):
            self.read()
        else:
            os.mkdir(self.folderName)

    def read(self) -> None:
        for entry in os.scandir(self.folderName):
            if entry.is_file():
                self.new(entry.name)

    def new(self, fileName: str) -> Database:
        db = self.dbClass(
            os.path.join(self.folderName, fileName),
            self.filePassword,
            self.fileCompressionClass,
            self.saveAtExit
        )
        self.update({fileName : db})
        return db

class DatabaseZip(dict):
    def __init__(self,
        zipName: str,
        filePassword: bytes = None,
        fileCompressionClass = None,
        saveAtExit: bool = True,
        dbClass: Database = DatabaseJSON
    ):
        self.zipName = zipName
        self.filePassword = filePassword
        self.fileCompressionClass = fileCompressionClass
        self.saveAtExit = saveAtExit
        self.dbClass = dbClass

        if zipfile.is_zipfile(self.zipName):
            self.read()

    def __del__(self):
        if self.saveAtExit:
            self.commit()

    def read(self) -> None:
        self.clear()

        with zipfile.ZipFile(self.zipName, "r") as archive:
            for fileName in archive.namelist():
                self.new(fileName, archive.read(fileName))

    def new(self, fileName: str, data: bytes = None) -> Database:
        db = self.dbClass(
            io.BytesIO(data),
            self.filePassword,
            self.fileCompressionClass,
            self.saveAtExit
        )
        self.update({fileName : db})
        return db

    def write(self) -> None:
        if os.path.isfile(self.zipName):
            os.remove(self.zipName)

        with zipfile.ZipFile(self.zipName, "w") as archive:
            for fileName, db in self.items():
                db.commit()
                archive.writestr(fileName, db.file.getvalue())

    def commit(self) -> None:
        self.write()

def test():
    p, c = b"1234", lzma
    h = ["firstname", "lastname", "age"]
    d = [[f"fn{i}", f"ln{i}", i] for i in range(1000)]

    db = Database("database", p, c)
    db["headers"] = h
    db["data"] = d

    dbxml = DatabaseXML("database.xml", p, c)
    dbxml["headers"] = h
    dbxml["data"] = d

    dbjson = DatabaseJSON("database.json", p, c)
    dbjson["headers"] = h
    dbjson["data"] = d

    dbyaml = DatabaseYAML("database.yaml", p, c)
    dbyaml["headers"] = h
    dbyaml["data"] = d

    dbcsv = DatabaseCSV("database.csv", p, c)
    dbcsv.headers = h
    dbcsv.data = d

    dbfolder = DatabaseFolder("databasefolder", p, c)
    db1 = dbfolder.new("database1")
    db1["headers"] = h
    db1["data"] = d

    dbzip = DatabaseZip("database.zip", p, c)
    db1 = dbzip.new("database1")
    db1["headers"] = h
    db1["data"] = d

if __name__ == "__main__":
    test()

