# A ideia deste script é ler os arquivos de uma pasta, organiza-los por data, criando pastas para ano e mes
# envia-los para o S3, e posteriormente para o S3 glacier
# as informações de acesso à AWS deverão ser lidos de um arquivo de configuração
# após o térmido do envio dos arquivos ao S3, a pasta deverá ser marcada como "BACKUPEADA"
# o modo de usar deverá ser:
# mys3backup.py PASTAORIGEM

import os
import sys
# from threading import local
from PIL import Image
from PIL.Image import Exif
from PIL.ExifTags import TAGS, GPSTAGS
import requests
import tempfile
import shutil
import boto3
from dotenv import load_dotenv

def get_exif(file_name) -> Exif:
    image: Image.Image = Image.open(file_name)
    return image.getexif()

def get_created_at(exif):
    for tagid in exif:
        tagname = TAGS.get(tagid, tagid)
        value = exif.get(tagid)
        if tagname == "DateTime":
            break
    return value

def parse_created_at(input_date):
    year = input_date[:4]
    month = input_date[5:7]
    day = input_date[8:10]

    return {"year": year, "month": month, "day": day}

def get_geo(exif):
    for key, value in TAGS.items():
        if value == "GPSInfo":
            break
    gps_info = exif.get_ifd(key)
    return {
        GPSTAGS.get(key, key): value for key, value in gps_info.items()
    }

def parse_geo(latitude_ref, latitude, longitude_ref, longitude):
    lat = float(latitude[0]) + (float(latitude[1]) / 60) + (float(latitude[2]) / 3600)
    long = float(longitude[0]) + (float(longitude[1]) / 60) + (float(longitude[2]) / 3600)
    if latitude_ref == "S":
        lat_ref = "-"
    if longitude_ref == "W":
        long_ref = "-"

    return_lat = "{}{}".format(lat_ref, lat)
    return_long = "{}{}".format(long_ref, long)
    return (return_lat, return_long)

def get_location(lat_long):
    latitude = lat_long[0]
    longitude = lat_long[1]
    APIKEY = "db378c3cab595944ca9c796ffff5055f"
    url = "http://api.openweathermap.org/geo/1.0/reverse?lat={}&lon={}&limit=5&appid={}".format(latitude, longitude, APIKEY)
    response = requests.get(url)
    data = response.json()
    return data[0]["name"]

def folder_path(created_at, location):
    folder = "{}/{}/{}".format(created_at["year"], created_at["month"], created_at["day"])
    if location:
        folder += "/{}".format(location)

    return folder

def s3_upload(local_directory, s3_path, bucket):
    ACCESS_KEY = os.getenv('ACCESS_KEY')
    SECRET_KEY = os.getenv('SECRET_KEY')
    client = boto3.client(
                            's3',
                            aws_access_key_id=ACCESS_KEY,
                            aws_secret_access_key=SECRET_KEY                     
                        )

    try:
        client.head_object(Bucket=bucket, Key=s3_path)
    except:
        client.upload_file(local_directory, bucket, s3_path)

def main():
    load_dotenv()
    #Le a pasta de origem
    folder = sys.argv[1]
    folder_content = os.scandir(folder)
    for content in folder_content:
        # image = Image.open(content.path)
        # print(content.path)
        exifdata = get_exif(content.path)
        created_at = parse_created_at(get_created_at(exifdata))
        geo = get_geo(exifdata)
        location = ""
        if geo:
            location = get_location(parse_geo(geo["GPSLatitudeRef"], geo["GPSLatitude"], geo["GPSLongitudeRef"], geo["GPSLongitude"]))

        folder = folder_path(created_at, location)
        path = os.path.join(tempfile.gettempdir(), "backup", folder)
        file_path = os.path.join(tempfile.gettempdir(), "backup", folder, content.name)
        s3_path = os.path.join(folder, content.name)
        print(s3_path)

        os.makedirs(path, exist_ok=True)

        shutil.copy2(content.path, file_path)
        # print(tempfile.gettempdir())

        bucket = os.getenv('bucket')
        s3_upload(file_path, s3_path, bucket)

if __name__ == "__main__":
    main()