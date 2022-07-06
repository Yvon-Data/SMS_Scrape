import scrapy
from sms.items import VinItem
from sms.items import DetailsItem
from scrapy.loader import ItemLoader
from collections import Counter
import pickle

usdot_list = [1]

# download_path = "C:/Users/Yvon/Documents/Status/"
#
# with open(download_path + 'active', 'rb') as act:
#     codes_active = pickle.load(act)

# START SPIDER CLASSES


# VIN Spider
class spider1(scrapy.Spider):
    name = "vin"
    website_warning = 0
    custom_settings = {
        'ITEM_PIPELINES': {
            'sms.pipelines.MongoDb_VIN': 100
        }
    }

    def start_requests(self):
        for usdot in usdot_list:
            vin_url = f"https://ai.fmcsa.dot.gov/SMS/Carrier/{usdot}/CompleteProfile.aspx"

            yield scrapy.Request(url=vin_url, callback=self.parse_main, meta={'usdot': usdot})

    def parse_main(self, response):

        # States Operated In
        scrape_registered = response.xpath('//*[@class="inspection"]/td[5]/text()').getall()
        scrape_inspection = response.xpath('//*[@class="inspection"]/td[3]/text()').getall()
        scrape_crash = response.xpath('//*[@class="crash"]/td[3]/text()').getall()

        state_registered = Counter(scrape_registered)
        state_inspection = Counter(scrape_inspection)
        state_crash = Counter(scrape_crash)

        # Load States into item before iterating VINs
        l = ItemLoader(item=VinItem(), selector=response)
        l.add_value("USDOT", str(response.meta['usdot']))
        l.add_value("State_Registered", state_registered)
        l.add_value("State_Inspection", state_inspection)
        l.add_value("State_Crash", state_crash)
        yield l.load_item()

        # VIN Links
        for inspection in response.xpath('//tr[@class="inspection"]/td[2]/a/@href'):
            inspection_url = response.urljoin(inspection.get())
            yield scrapy.Request(inspection_url, callback=self.parse_units)

    # Parsing VIN Inspection URL
    def parse_units(self, response):

        unit_list = response.xpath('//table[@id="vehicleTable"]/tbody/tr')

        for unit in unit_list:
            usdot = response.xpath('//div[@id="cInfoPnl"]/ul/li[2]/span/text()').get()
            vin = unit.xpath('td[6]/text()').get()
            vehicle_type = unit.xpath('td[2]/text()').get()

            vin_api_url = f"https://vpic.nhtsa.dot.gov/api/vehicles/decodevinvaluesextended/{vin}?format=json"

            yield scrapy.Request(url=vin_api_url, callback=self.parse_vin_api,
                                 meta=
                                 {
                                     'vin': vin,
                                     'usdot': usdot,
                                     'vehicle_type': vehicle_type
                                 })

    # Parsing VIN API for Vehicle Information
    def parse_vin_api(self, response):

        l = ItemLoader(item=VinItem(), selector=response)

        decoded = response.json()
        results = decoded["Results"][0]

        l.add_value("_id", response.meta['vin']),
        l.add_value("USDOT", response.meta['usdot']),
        l.add_value("Type", response.meta['vehicle_type'])

        l.add_value("VIN", results.get("VIN")),
        l.add_value("Vehicle_Brand", results.get("Make")),
        l.add_value("Vehicle_Model", results.get("Model")),
        l.add_value("Vehicle_Year", results.get("ModelYear")),
        l.add_value("Vehicle_GVWR", results.get("GVWR")),
        l.add_value("Vehicle_Type", results.get("BodyClass")),
        l.add_value("Engine_Manufacturer", results.get("EngineManufacturer")),
        l.add_value("Engine_Model", results.get("EngineModel")),
        l.add_value("Engine_DisplacementL", results.get("DisplacementL")),
        l.add_value("Engine_Cylinders", results.get("EngineCylinders")),
        l.add_value("Engine_Configuration", results.get("EngineConfiguration")),
        l.add_value("Engine_Fuel", results.get("FuelTypePrimary"))

        yield l.load_item()


# Details Spider
class spider2(scrapy.Spider):
    name = "details"
    website_warning = 0
    custom_settings = {
        'ITEM_PIPELINES': {
            'sms.pipelines.MongoDb_Details': 100
        }
    }

    def start_requests(self):
        for usdot in usdot_list:
            overview_details_url = f"https://ai.fmcsa.dot.gov/SMS/Carrier/{usdot}/Overview.aspx?FirstView=True"
            yield scrapy.Request(url=overview_details_url, callback=self.overview_details, meta={'usdot': usdot})

    # Overview Details
    def overview_details(self, response):

        insurance = []
        for i in range(1, 5):
            insurance_type = response.xpath(f'normalize-space(//*[@id="LicensingAndInsurance"]/table/tbody/tr[{i}]/th/text())').get()
            binary = response.xpath(f'//*[@id="LicensingAndInsurance"]/table/tbody/tr[{i}]/td[1]/text()').get()
            mc_mx = response.xpath(f'//*[@id="LicensingAndInsurance"]/table/tbody/tr[{i}]/td[2]/text()').get()

            if mc_mx:
                insurance += [f'{insurance_type}: {binary} under authority {mc_mx}']
            else:
                insurance += [f'{insurance_type}: {binary}']

        usdot = response.meta['usdot']
        registration_details_url = f"https://ai.fmcsa.dot.gov/SMS/Carrier/{usdot}/CarrierRegistration.aspx"

        yield scrapy.Request(url=registration_details_url, callback=self.registration_details,
                             meta={
                                 'insurance': insurance,
                                 'usdot': usdot
                             })

    # Parsing Registration Details URL
    def registration_details(self, response):

        l = ItemLoader(item=DetailsItem(), selector=response)

        # Calculating Owned/ Leased percentages
        owned = response.xpath('//table/tbody/tr/td[1]/text()').getall()

        owned_int = [string.replace(',', '') for string in owned]
        owned_sum = sum(list(map(int, owned_int)))

        term_leased = response.xpath('//table/tbody/tr/td[2]/text()').getall()
        term_int = [string.replace(',', '') for string in term_leased]
        term_leased_sum = sum(list(map(int, term_int)))

        total = sum([owned_sum, term_leased_sum])

        owned_percentage = str(int(owned_sum / total * 100))
        term_leased_percentage = str(int(term_leased_sum / total * 100))

        l.add_value('Vehicles_Owned_Percent', owned_percentage)
        l.add_value('Vehicles_Leased_Percent', term_leased_percentage)

        # Adding Insurance to Item
        for insurance in response.meta['insurance']:
            l.add_value('Insurance', insurance)

        l.add_xpath('USDOT', '//*[@id="regBox"]/ul[1]/li[3]/span/text()')
        l.add_xpath('Phone', '//*[@id="regBox"]/ul[1]/li[5]/span/text()')
        l.add_xpath('Fax', '//*[@id="regBox"]/ul[1]/li[6]/span/text()')
        l.add_xpath('Email', '//*[@id="regBox"]/ul[1]/li[7]/span/text()')

        # Vehicle Breakdown
        for i in range(1, 16):
            vehicle_type = response.xpath(f'//*[@id="regBox"]/table/tbody/tr[{i}]/th/text()').get()
            owned = response.xpath(f'//*[@id="regBox"]/table/tbody/tr[{i}]/td[1]/text()').get()
            term = response.xpath(f'//*[@id="regBox"]/table/tbody/tr[{i}]/td[2]/text()').get()
            trip = response.xpath(f'//*[@id="regBox"]/table/tbody/tr[{i}]/td[3]/text()').get()

            if owned != '0':
                l.add_value('owned_breakdown', vehicle_type + ': ' + owned)

            if term != '0':
                l.add_value('term_breakdown', vehicle_type + ': ' + term)

            if trip != '0':
                l.add_value('trip_breakdown', vehicle_type + ': ' + trip)

        yield l.load_item()
