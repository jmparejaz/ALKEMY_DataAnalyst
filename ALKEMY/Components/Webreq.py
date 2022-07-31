import requests
import pandas as pd
from bs4 import BeautifulSoup
import io
import os



class Webreq:
    def __init__(self, url, key: str):
        self.url = url
        self.key = key

    def requests_downloadurl(self):
        r = requests.get(self.url)
        soup = BeautifulSoup(r.content, "html.parser")
        a = soup.find("a", "btn btn-green btn-block")
        url_download = str(a).split('"')[3]
        con = requests.get(url_download)
        content = con.content
        df = pd.read_csv(io.StringIO(content.decode("utf-8")))
        tabla = soup.find_all("div", "col-xs-8 value")
        tabla = pd.Series(tabla)
        ultimocambio = str(tabla.iloc[-1])
        fecha = ultimocambio.split("\n")[-3].split(" ")
        fecha_spa = pd.Series([x for x in fecha if x != "" and x != "de"])
        fecha_eng = fecha_spa.replace(
            {
                "enero": "January",
                "febrero": "February",
                "marzo": "March",
                "abril": "April",
                "mayo": "May",
                "junio": "June",
                "julio": "July",
                "agosto": "August",
                "septiembre": "September",
                "octubre": "October",
                "noviembre": "November",
                "diciembre": "December",
            }
        )
        fecha_num = fecha_spa.replace(
            {
                "enero": "01",
                "febrero": "02",
                "marzo": "03",
                "abril": "04",
                "mayo": "05",
                "junio": "06",
                "julio": "07",
                "agosto": "08",
                "septiembre": "09",
                "octubre": "10",
                "noviembre": "11",
                "diciembre": "12",
            }
        )
        return df, fecha_spa, fecha_eng, fecha_num

    def make_dirs(self, data, date_spa, date_eng, date_num):
        current = os.getcwd()
        path_cat = os.path.join(current, self.key)

        try:
            os.mkdir(path_cat)
        except:
            pass

        folder1 = "%s-%s" % (date_spa[2], date_spa[1])
        path_cat_anio_mes = os.path.join(path_cat, folder1)

        try:
            os.mkdir(path_cat_anio_mes)
        except:
            pass

        filename = "%s-%s-%s-%s.csv" % (self.key, date_num[0], date_num[1], date_num[2])
        path_filename = os.path.join(path_cat, folder1, filename)
        data.to_csv(path_filename)
