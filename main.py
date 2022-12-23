from flask import Flask
from flask_cors import CORS
import os
from notion_client import Client
from notion_client.helpers import collect_paginated_api
from datetime import datetime, timedelta


notion = Client(auth=os.environ["NOTION_TOKEN"])

app = Flask(__name__)
CORS(app)


#The Notion database "Promos compartidas" - https://www.notion.so/exploradordeviajes/459497a1b8f9438ebff5034915a943dd?v=8af7c0f4179f47a4b98b91ffa0980c25
promos_compartidas_database_id = "459497a1-b8f9-438e-bff5-034915a943dd"
#The Notion database "Ciudades" - 
ciudades_database_id = "da4ffb2a-4aa0-4301-bbf5-bc0d40eff92c"
gratuitos_and_premium_name = "Gratuitos y Premium"
prop_last_shared_date_premium = "Premium - Última fecha compartida"
prop_last_shared_date_gratuitos = "Básica - Última fecha compartida"


#Gets the date one year ago
def oneYearAgo():
  # Get the current date
  now = datetime.now()
  # Calculate the date one year ago by subtracting a timedelta object
  one_year_ago = now - timedelta(days=365)
  return one_year_ago.strftime('%Y-%m-%d')

  
#Dates keepers
oldest_day_limit = oneYearAgo() #The limit is a year ago
last_date_city_shared_to_premium = {}
last_date_city_shared_to_gratuitos = {}

#####################################################################################
### GET THE LAST DAYS WHERE A PROMO TO A CITY WAS SHARED TO GRATUITOS AND PREMIUM ###
#####################################################################################
promos_compartidas_database_query = {
  "database_id": promos_compartidas_database_id,
  "filter": {
    "and": [
      {
        "property": "Fecha",
        "date": {
          "on_or_after": oldest_day_limit
        }
      }
    ]
  },
  "sorts": [
    {
      "property": "Fecha",
      "direction": "ascending"
    }
  ]
}


def updateCityAndDateMap(promo_audience, promo_city, promo_date):
  if promo_audience == gratuitos_and_premium_name:
    last_date_city_shared_to_gratuitos[promo_city] = promo_date
    last_date_city_shared_to_premium[promo_city] = promo_date
  else:
    last_date_city_shared_to_premium[promo_city] = promo_date


def keyIsPresentInList(key, dict):
  return key in dict


def mapLastSharedDate(promos):
  for promo in promos:
    curr_audiencia_select = promo["properties"]["Audiencia"]["select"]
    if curr_audiencia_select is not None:
      curr_promo_audiencia_name = curr_audiencia_select = promo["properties"]["Audiencia"]["select"]["name"]
      curr_date = promo["properties"]["Fecha"]["date"]["start"]
      ids = [obj['id'] for obj in promo["properties"]['🏙️ Ciudades']['relation']]
      if len(ids) == 1:
        updateCityAndDateMap(curr_promo_audiencia_name, ids[0], curr_date)
      

def mapLastSharedDateFromPromosCompartidas():
    print("Getting dates from Promos Compartidas database...")
    response = notion.databases.query(**{
        "database_id": promos_compartidas_database_id,
        "filter": {
          "and": [
            {
              "property": "Fecha",
              "date": {
                "on_or_after": oldest_day_limit
              }
            }
          ]
        },
        "sorts": [
          {
            "property": "Fecha",
            "direction": "ascending"
          }
        ]
      })
    results = response["results"]
    mapLastSharedDate(results)
    print("Finished mapping all the dates. Found ", len(last_date_city_shared_to_premium), " dates for premium and ", len(last_date_city_shared_to_gratuitos), " for gratuitos.")


########################################################################################################
### UPDATE THE LAST SHARED DATE FOR PROMOS TO GRATUITOS AND PREMIUM IN THE CIUDADES NOTION DATABASE  ###
########################################################################################################


def update_last_gratuitos_shared_date(city_id, city_name, new_date):
  print("Updating city", city_name, "with date", new_date, "for gratuitos...")
  properties_to_update = {
    prop_last_shared_date_gratuitos: {
        "date": {
            "start": new_date
        }
    }
  }
  notion.pages.update(city_id, properties=properties_to_update)
  print("City", city_name, "updated.")
  
  

def check_gratuitos_date_for_city(city_id, city_name, city):
  city_last_time_shared = city["properties"][prop_last_shared_date_gratuitos]["date"]
  #If the city has never shared before
  if city_last_time_shared is None:
    #City has no last shared dates for Gratuitos, so it will be set to 1 year
    update_last_gratuitos_shared_date(city_id, city_name, oldest_day_limit)
  else:
    city_last_time_shared = city["properties"][prop_last_shared_date_gratuitos]["date"]["start"]
    #If the city has been shared to the gratuitos members in the last year
    if city_id in last_date_city_shared_to_gratuitos:
      if last_date_city_shared_to_gratuitos[city_id] == city_last_time_shared:
        print("Dates are the same, so it is not going to be updated for Gratuitos")
      else:
        update_last_gratuitos_shared_date(city_id, city_name, last_date_city_shared_to_gratuitos[city_id])
    else:
      #City has not been shared to Gratuitos last year, so the date will be set to 1 year ago
      update_last_gratuitos_shared_date(city_id, city_name, oldest_day_limit)


def update_last_premium_shared_date(city_id, city_name, new_date):
  print("Updating city", city_name, "with date", new_date, "for premium...")
  properties_to_update = {
    prop_last_shared_date_premium: {
        "date": {
            "start": new_date
        }
    }
  }
  notion.pages.update(city_id, properties=properties_to_update)
  print("City", city_name, "updated.")


def check_premium_date_for_city(city_id, city_name, city):
  city_last_time_shared = city["properties"][prop_last_shared_date_premium]["date"]
  #If the city has never shared before
  if city_last_time_shared is None:
    #City has no last shared dates for Premium, so it will be set to 1 year
    update_last_premium_shared_date(city_id, city_name, oldest_day_limit)
  else:
    city_last_time_shared = city["properties"][prop_last_shared_date_premium]["date"]["start"]
    #If the city has been shared to the premium members in the last year
    if city_id in last_date_city_shared_to_premium:
      if last_date_city_shared_to_premium[city_id] == city_last_time_shared:
        print("Dates are the same, so it is not going to be updated for Premium")
      else:
        update_last_premium_shared_date(city_id, city_name, last_date_city_shared_to_premium[city_id])
    else:
      #City has not been shared to Premium last year, so the date will be set to 1 year ago
      update_last_premium_shared_date(city_id, city_name, oldest_day_limit)


def update_last_shared_dates_in_city(city):
  city_id = city["id"]
  city_name = city["properties"]["Name"]["title"][0]["text"]["content"]
  print("===")
  print("City", city_name)
  check_gratuitos_date_for_city(city_id, city_name, city)
  check_premium_date_for_city(city_id, city_name, city)
  

def update_last_shared_dates_in_ciudades():
  print("Updating the Ciudades database with the last shared dates...")
  for result in collect_paginated_api(notion.databases.query, database_id=ciudades_database_id):
    update_last_shared_dates_in_city(result)
    

@app.route('/')
def index():
  mapLastSharedDateFromPromosCompartidas()
  update_last_shared_dates_in_ciudades()
  return {"status":200}


app.run(host='0.0.0.0', port=81)
