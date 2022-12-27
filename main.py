from flask import Flask
from flask_cors import CORS
import os
from notion_client import Client
import time
from pprint import pprint
import requests

notion = Client(auth=os.environ["NOTION_TOKEN"])

app = Flask(__name__)
CORS(app)

#Axiom.ai
axiom_api_key = os.environ["AXIOM_API_KEY"]
axiom_name_to_clear_verification_sheet = "Clear Sheet - Kayak data from verification"
axiom_name_to_scrap_kayak_results = "Scrap Kayak results"

#The Notion database "Vuelos" - https://www.notion.so/exploradordeviajes/76dea96154c14c2e9ee606de094f0c9c?v=3af9e9b1a8724910a86bda1ac8e90b36
vuelos_database_id = "76dea961-54c1-4c2e-9ee6-06de094f0c9c"

#################################################
### 1. FETCHES THE TOP PROMOS BASED ON PROFIT ###
#################################################


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
  return result["results"][:items_count]


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
  response = requestAxiomWithRetryAndDelay(request)
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
    "data": [verification_data]
  }

  response = requestAxiomWithRetryAndDelay(request)
  print(response.json())


def verifyPromos(promos):
  clearVerificationSheet()
  promos_data_for_request = generateDataForRequest(promos)
  for request_data in promos_data_for_request:
    verifyPromoInAxiom(request_data)


#######################################################################
### 3. CALLS THE REPLIT THAT MIGRATES FROM GOOGLE SHEETS TO NOTION  ###
#######################################################################

def callMigrator():
  requests.get("https://gs2notion-migrator.altancabal.repl.co/migrate")


@app.route('/')
def index():
  return "Use the route /verify to verify promos"


@app.route('/verify')
def verify():
  #1. FETCHES THE TOP PROMOS BASED ON PROFIT
  promos = fetchTopPromos(10)
  #2. CALLS THE AXIOM THAT VERIFIES THE PROMOS
  verifyPromos(promos)

  #3. CALLS THE REPLIT THAT MIGRATES FROM GOOGLE SHEETS TO NOTION
  print("Waiting 5 minutes before migrating data from Google Sheets to Notion")
  time.sleep(300)
  callMigrator()
  
  return {"status": 200}


app.run(host='0.0.0.0', port=81)
