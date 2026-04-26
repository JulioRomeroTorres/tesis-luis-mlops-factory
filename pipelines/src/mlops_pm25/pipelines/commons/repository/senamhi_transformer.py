import re
import json
import requests
from typing import Optional

class SenamhiTransformer:
  def __init__(self, base_url: str):
    self.base_url = base_url

  def parse_highcharts_config(self, raw_information: str) -> str:

    raw_information = re.sub(r'/\*.*?\*/', '', raw_information, flags=re.DOTALL)
    raw_information = re.sub(r'//.*?(\n|$)', '', raw_information)

    raw_information = re.sub(r'([{,])\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*:', r'\1"\2":', raw_information)

    raw_information = re.sub(r',\s*}', '}', raw_information)
    raw_information = re.sub(r',\s*]', ']', raw_information)

    raw_information = re.sub(r"'([^']*)'", r'"\1"', raw_information)
    return raw_information

  def get_json_information_from_bytes(self, raw_bytes_information):

      if isinstance(raw_bytes_information, bytes):
        print("Transform bytes to string", raw_bytes_information)
        try:
          raw_bytes_information = raw_bytes_information.decode('utf-8')
        except Exception as e:
          raw_bytes_information = raw_bytes_information.decode('utf-8', errors='replace')


      current_match = re.search(r"Highcharts\.chart\('container',\s*(\{.*\})\);", raw_bytes_information, re.DOTALL)
      diccionario_str = ""

      if current_match:
          diccionario_str = current_match.group(1)
          diccionario_str = self.parse_highcharts_config(diccionario_str)
          try:
              config = json.loads(diccionario_str)
              print("✅ Éxito!")

              time_line = config['xAxis']['categories']
              data = config['series'][0]['data']

              return time_line, data

          except json.JSONDecodeError as e:
              print(f"Error en posición {e.pos}: {e.msg}")
              print(f"Fragmento problemático: ...{diccionario_str[e.pos-50:e.pos+50]}...")
              raise e


  def get_variable_data(
      self, station_id: str, variable_name: str,
      start_period: Optional[str] = '01/03/2025', end_period: Optional[str] = '01/03/2026'
  ):
    query_params = {
      "estacion": station_id,
      "cont": variable_name,
      "f1": start_period,
      "f2": end_period
    }
    try:
      print(f"Obteniendo valores para {station_id} y {variable_name}")
      raw_response = requests.get(self.base_url, params=query_params).content
      print("Exito al invocar al servicio")
      raw_response = self.get_json_information_from_bytes(raw_response)

      return raw_response
    except Exception as e:
      print(f"Error to get information to {station_id} and {variable_name} con detalle {e}")
      raise e
