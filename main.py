from flask import Flask
from flask_cors import CORS
import os
from notion_client import Client
import time
from pprint import pprint
import requests
from threading import Thread

notion = Client(auth=os.environ["NOTION_TOKEN"])

app = Flask(__name__)
CORS(app)

#Axiom.ai
axiom_api_key = os.environ["AXIOM_API_KEY"]
axiom_name_to_clear_verification_sheet = "Clear Sheet - Kayak data from verification"
axiom_name_to_scrap_kayak_results = "Get Kayak promo details"

#The Notion database "Vuelos" - https://www.notion.so/exploradordeviajes/76dea96154c14c2e9ee606de094f0c9c?v=3af9e9b1a8724910a86bda1ac8e90b36
vuelos_database_id = "76dea961-54c1-4c2e-9ee6-06de094f0c9c"

#################################################
### 1. FETCHES THE TOP PROMOS BASED ON PROFIT ###
#################################################
def isPriceValidForRegion(region, price):
  if region == "Europa" and price < 650:
    return True
  elif (region == "Sudamérica Norte" or region == "Estados Unidos Centro" or region == "México" or region == "Estados Unidos Este" or region == "Canadá") and price < 300:
    return True
  elif region == "Estados Unidos Oeste" and price < 600:
    return True
  elif region == "Sudamérica Sur" and price < 500:
    return True
  elif region == "África" and price < 800:
    return True
  elif (region == "Oceanía" or region == "Asia" or region == "Medio Oriente") and price < 1000:
    return True
  return False


def validPromo(promo):
  region = promo["properties"]["Región"]["rollup"]["array"][0]["select"]["name"]
  price = promo["properties"]["price"]["number"]
  return isPriceValidForRegion(region, price)


def getPromosWithDesiredParameters(results):
  verifiedResults = []
  for result in results:
    if validPromo(result):
      verifiedResults.append(result)
  return verifiedResults


def fetchTopPromos(items_count):
  result = notion.databases.query(
    **{
      "database_id":
      vuelos_database_id,
      "filter": {
        "and": [
          {
            "property": "source",
            "select": {
              "equals": "kayak-explore-scraped"
            }
          },
          {
                "property": "discountSpecialFunction",
                "formula": {
                    "number": {
                        "greater_than": 0
                    }
                }
          }
        ]
      },
      "sorts": [{
        "property": "discountSpecialFunction",
        "direction": "descending"
      }]
    })

  all_results = result["results"]
  validatedPromos = getPromosWithDesiredParameters(all_results)
  print("From a total of", len(all_results), "results," , len(validatedPromos), "are valid and the maximum to return are", items_count)
  return validatedPromos[:items_count]


###################################################
### 2. CALLS THE AXIOM THAT VERIFIES THE PROMOS ###
###################################################

def requestAxiomWithRetryAndDelay(request):
  retry_count = 0
  retry_delay = 5 #seconds

  while True:
    print("Executing the request to Axiom...")
    response = requests.post("https://lar.axiom.ai/api/v3/trigger", json=request)

    # Check the status code of the response
    if response.status_code == 503:
      if retry_count >= 10:
        print("Retried 10 times, but stopping because Axiom is still busy")  
        #Retries a maximum of 10 times
        return None
      
      # Increment the retry count and the retry delay
      retry_count += 1
      retry_delay *= 2

      print("Axiom is busy, retrying in", retry_delay, "seconds ...")  
      time.sleep(retry_delay)
      
    else:
        return response


def clearVerificationSheet():
  print("Cleaning sheet...")
  request = {
    "key": axiom_api_key,
    "name": axiom_name_to_clear_verification_sheet,
    "data": [[""]]
  }
  #Request Axiom. If axiom is busy, just ignore it
  response = requests.post("https://lar.axiom.ai/api/v3/trigger", json=request)
  print("Response from Axiom cleaning sheet: ", response.text)
  

def generateDataForRequest(promos):
  promos_data_for_request = []
  for promo in promos:
    promo_id = promo["id"]
    promo_url = promo["properties"]["kayakUrl"]["url"]
    promo_data = [promo_id, promo_url]
    promos_data_for_request.append(promo_data)
  return promos_data_for_request


def verifyPromoInAxiom(verification_data):
  print("Requesting to scrap the promo ", verification_data, "...")
  request = {
    "key": axiom_api_key,
    "name": axiom_name_to_scrap_kayak_results,
    "data": verification_data
  }

  response = requestAxiomWithRetryAndDelay(request)
  print(response.json())


def verifyPromos(promos):
  clearVerificationSheet()
  promos_data_for_request = generateDataForRequest(promos)
  print("promos_data_for_request")
  print(promos_data_for_request)
  verifyPromoInAxiom(promos_data_for_request)


#######################################################################
### 3. CALLS THE REPLIT THAT MIGRATES FROM GOOGLE SHEETS TO NOTION  ###
#######################################################################

def callMigrator():
  requests.get("https://gs2notion-migrator.altancabal.repl.co/migrate")

def startFetching():
  #1. FETCHES THE TOP PROMOS BASED ON discountSpecialFunction
  promos = fetchTopPromos(10)
  #2. CALLS THE AXIOM THAT VERIFIES THE PROMOS
  verifyPromos(promos)


@app.route('/')
def index():
  return "Use the route /verify to verify promos"


@app.route('/verify')
def verify():
  #Asyncronously start to fetch promos
  Thread(target = startFetching).start()
  #startFetching()
  return {"message": "Started to run Axioms. This process will take long. Check the axiom extension to see more"}


app.run(host='0.0.0.0', port=81, processes=1)
