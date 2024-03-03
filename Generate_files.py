import os
import hashlib
import logging
import random
import string
import country_list
import countryinfo
import pyspark.sql.functions as F
import pyspark.sql.types as T

from pyspark.sql import SparkSession
from datetime import date, datetime, timedelta
from uuid import uuid4
from google.colab import drive

def generate_digits(count: int = 10) -> str:
    """Генерирует число по заданному количеству цифр.

    Parameters:
        count: int
            Количество цифр в числе.

    Returns:
        str:
            Сгенерированное число.
    """
    return "".join(random.choices(string.digits, k=count))

def generate_rows_table(
    count: int = 1,
    timestampcolumn: datetime = date.today()
) -> list:
    """Генерует строки таблицы.

    Parameters:
        count: int
            Количество строк.
        timestampcolumn: datetime
            Дата партиции.

    Returns:
        list_cookies: list
            Возвращает массив списков, представляющий собой строку таблицы.
    """

    list_cookies = list()
    
    for _ in range(count):
        # генерация cookies
        inn = generate_digits(12)

        _sa_cookie_a = {
            "key": "_sa_cookie_a",
            "value": f"SA1.{uuid4()}.{generate_digits(10)}"
        }

        _fa_cookie_a = {
            "key": "_fa_cookie_a",
            "value": f"ads2.{generate_digits(10)}.{generate_digits(10)}"
        }

        _ym_cookie_c = {
            "key": "_ym_cookie_c",
            "value": generate_digits(20)
        }

        _fbp = {
            "key": "_fbp",
            "value": f"fb.{random.choice(string.digits)}."
                     f"{generate_digits(13)}."
                     f"{generate_digits(9)}"
        }

        org_uid = {
            "key": "org_uid",
            "value": generate_digits(7)
        }

        user_uid = {
            "key": "user_uid",
            "value": generate_digits(7)
        }

        user_phone = {
            "key": "user_phone",
            "value": f"79{generate_digits(9)}"
        }

        user_mail = {
            "key": "user_mail",
            "value": f"""{''.join(random.choices(string.ascii_letters +
                             string.digits, k=10))}@user.io"""
        }

        # генерация event_type
        event_type = random.choice(["SUBMIT", "REGISTER", "SUBMIT_MD5"])

        # генерация event_action
        event_action = random.choice(["pageview", "event", "login-check-otp"])

        # генерация data_value
        if event_type == "SUBMIT":
            data_value = hashlib.sha256(bytes(inn, encoding="utf-8")).hexdigest()
        elif event_type == "SUBMIT_MD5":
            data_value = hashlib.md5(bytes(inn, encoding="utf-8")).hexdigest()
        else:
            data_value = None

        # генерация страны, города и геопозиции
        while True:
            try:
                _, geocountry = random.choice(countries)
                country = countryinfo.CountryInfo(geocountry)
                city = country.capital()
                geoaltitude = ",".join(map(str, country.latlng()))
                break
            except KeyError as e:
                logger.error(f"В списке 'country' отсутствует значение {str(e)}")

        # Генерация meta_platform
        meta_platform = random.choice(["WEB", "MOBAIL"])

        # Генерация user_os
        if meta_platform == "WEB":
            user_os = random.choice(["Mac", "Windows", "Ubuntu"])
        else:
            user_os = random.choice(["IOS", "Android", "HarmonyOS", "BlackBerryOS"])
        # Генерация systemlanguage
        systemlanguage = random.choice(["RU", "ENG"])

        # Генерация screensize
        screensize = "1920x1080"

        # Добавление спосков в единый список
        list_cookies.append([
            inn,
            [
                _sa_cookie_a,
                _fa_cookie_a,
                _ym_cookie_c,
                _fbp,
                org_uid,
                user_uid,
                user_phone,
                user_mail
            ],
            event_type,
            event_action,
            data_value,
            geocountry,
            city,
            user_os,
            systemlanguage,
            geoaltitude,
            meta_platform,
            screensize,
            timestampcolumn
        ])
    return list_cookies


spark = SparkSession.builder.master("local[*]").getOrCreate()
spark.conf.set("spark.sql.repl.eagerEval.enabled", True)  

drive.mount("/content/gdrive")

logger = logging.getLogger()
logging.basicConfig(
    filename = "mylog.log",
    format = "%(asctime)s - %(levelname)s - %(funcName)s: %(lineno)d - %(message)s",
    datefmt='%H:%M:%S',
)

countries = list(country_list.countries_for_language("en"))

schema = T.StructType([
    T.StructField("inn", T.StringType(), True),
    T.StructField(
        "raw_cookie",
        T.ArrayType(
            T.MapType(
                T.StringType(),
                T.StringType()
            )
        )
    ),
    T.StructField("event_type", T.StringType(), True),
    T.StructField("event_action", T.StringType(), True),
    T.StructField("data_value", T.StringType(), True),
    T.StructField("geocountry", T.StringType(), True),
    T.StructField("city", T.StringType(), True),
    T.StructField("user_os", T.StringType(), True),
    T.StructField("systemlanguage", T.StringType(), True),
    T.StructField("geoaltitude", T.StringType(), True),
    T.StructField("meta_platform", T.StringType(), True),
    T.StructField("screensize", T.StringType(), True),
    T.StructField("timestampcolumn", T.DateType(), True)
])

current_date = date.today()
folder = "/content/gdrive/MyDrive/data/json/"

if not os.path.isdir(folder):
    os.makedirs(folder)
    
i = 0

while i < 10:
    for _ in range(random.randrange(5, 10)):
        df = spark.createDataFrame(
            generate_rows_table(30, current_date),
            schema=schema
        )
        
        df.coalesce(1).write.mode("append") \
          .json(f"{folder}{current_date.strftime('%Y_%m_%d')}")

    current_date += timedelta(1)
    i += 1

    spark.stop()