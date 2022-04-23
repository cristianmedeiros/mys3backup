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
from PIL import UnidentifiedImageError
import datetime
import requests
import tempfile
import shutil
import boto3
from dotenv import load_dotenv

def get_exif(file_name) -> Exif:
    try:

        image: Image.Image = Image.open(file_name)

    except UnidentifiedImageError:
        raise
    except OSError:
        raise

    return image.getexif()

def get_created_at(exif, file_path):
    if exif:
        for tagid in exif:
            tagname = TAGS.get(tagid, tagid)
            value = exif.get(tagid)

            if tagname == "DateTime":
                break
    if not exif or not tagname == "DateTime" or value.isascii():
        # There's no metadata available, get date from file
        value = datetime.datetime.fromtimestamp(os.path.getmtime(file_path)).strftime("%Y:%m:%d")

    return value

def parse_created_at(input_date):
    year = input_date[0:4]
    month = input_date[5:7]
    day = input_date[8:10]

    return {"year": year, "month": month, "day": day}

def get_geo(exif):
    if exif:
        for key, value in TAGS.items():
            if value == "GPSInfo":
                break

        gps_info = exif.get_ifd(key)
        gps_data = {
            GPSTAGS.get(key, key): value for key, value in gps_info.items()
        }

        if 'GPSLatitude' not in gps_data:
            gps_data = False
      
    else:
        gps_data = False

    return gps_data

def parse_geo(latitude_ref, latitude, longitude_ref, longitude):
    try:
        lat = float(latitude[0]) + (float(latitude[1]) / 60) + (float(latitude[2]) / 3600)
        long = float(longitude[0]) + (float(longitude[1]) / 60) + (float(longitude[2]) / 3600)
        if latitude_ref == "S":
            latitude_ref = "-"
        if longitude_ref == "W":
            longitude_ref = "-"

        return_lat = "{}{}".format(latitude_ref, lat)
        return_long = "{}{}".format(longitude_ref, long)
        value = (return_lat, return_long)
    except:
        value = False

    return value

def get_location(lat_long):
    latitude = lat_long[0]
    longitude = lat_long[1]
    APIKEY = os.getenv('openweather_apikey')
    url = "http://api.openweathermap.org/geo/1.0/reverse?lat={}&lon={}&limit=5&appid={}".format(latitude, longitude, APIKEY)
    response = requests.get(url)
    data = False
    if response.status_code == 200:
        data_json = response.json()
        data = data_json[0]["name"]

    return data

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
        client.upload_file(local_directory, bucket, s3_path, ExtraArgs={'StorageClass': 'DEEP_ARCHIVE'})
        print("File saved: {}".format(s3_path))

def main():
    load_dotenv()
    #Le a pasta de origem
    folder = sys.argv[1]
    folder_content = os.walk(folder)
    bucket = os.getenv('bucket')

    for dirName, subdirList, fileList in folder_content:
        for fname in fileList:
            origin_file_path = os.path.join(dirName, fname)

            if os.path.splitext(origin_file_path)[-1].lower() in ['.jpg', '.jpeg', '.png']:

                try:
                    exifdata = get_exif(origin_file_path)
                    if exifdata:
                        data_created_at = get_created_at(exifdata, origin_file_path)
                        created_at = parse_created_at(data_created_at)
                    
                        geo = get_geo(exifdata)

                        location = ""
                        if geo:
                            geo_parse = parse_geo(geo["GPSLatitudeRef"], geo["GPSLatitude"], geo["GPSLongitudeRef"], geo["GPSLongitude"])
                            if geo_parse:
                                location = get_location(geo_parse)

                        folder = folder_path(created_at, location)

                        path = os.path.join(tempfile.gettempdir(), "backup", folder)
                        destiny_file_path = os.path.join(tempfile.gettempdir(), "backup", folder, fname)
                        s3_path = os.path.join(folder, fname)

                        os.makedirs(path, exist_ok=True)

                        shutil.copy2(origin_file_path, destiny_file_path)
                        s3_upload(destiny_file_path, s3_path, bucket)

                except UnidentifiedImageError:
                    continue
                except OSError:
                    continue
                except:
                    print("Erro na imagem: {} Message: {}".format(origin_file_path, sys.exc_info()))
            break
if __name__ == "__main__":
    main()