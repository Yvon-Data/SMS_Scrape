# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy
from itemloaders.processors import TakeFirst, MapCompose, Join
from w3lib.html import remove_tags, replace_escape_chars, replace_tags


def clean(text):
    s0 = remove_tags(text, keep=('br',))
    s1 = replace_tags(s0, ', ')
    s2 = replace_escape_chars(s1, which_ones=('\n', '\r', '\xa0'))
    return " ".join(s2.split())


def class_clean(text):
    return text[0].split(':', 1)[0]


def drop_na(text):
    return text


class VinItem(scrapy.Item):
    _id = scrapy.Field(output_processor=TakeFirst())
    USDOT = scrapy.Field(output_processor=TakeFirst())
    Type = scrapy.Field(input_processor=drop_na, output_processor=TakeFirst())
    VIN = scrapy.Field(input_processor=drop_na, output_processor=TakeFirst())
    Vehicle_Brand = scrapy.Field(input_processor=drop_na, output_processor=TakeFirst())
    Vehicle_Model = scrapy.Field(input_processor=drop_na, output_processor=TakeFirst())
    Vehicle_Year = scrapy.Field(input_processor=drop_na, output_processor=TakeFirst())
    Vehicle_GVWR = scrapy.Field(input_processor=class_clean, output_processor=TakeFirst())
    Vehicle_Type = scrapy.Field(input_processor=drop_na, output_processor=TakeFirst())
    Engine_Manufacturer = scrapy.Field(input_processor=drop_na, output_processor=TakeFirst())
    Engine_Model = scrapy.Field(input_processor=drop_na, output_processor=TakeFirst())
    Engine_DisplacementL = scrapy.Field(input_processor=drop_na, output_processor=TakeFirst())
    Engine_Cylinders = scrapy.Field(input_processor=drop_na, output_processor=TakeFirst())
    Engine_Configuration = scrapy.Field(input_processor=drop_na, output_processor=TakeFirst())
    Engine_Fuel = scrapy.Field(input_processor=drop_na, output_processor=TakeFirst())
    State_Registered = scrapy.Field()
    State_Inspection = scrapy.Field()
    State_Crash = scrapy.Field()
    pass


class DetailsItem(scrapy.Item):
    USDOT = scrapy.Field(output_processor=TakeFirst())
    Phone = scrapy.Field(output_processor=TakeFirst())
    Fax = scrapy.Field(output_processor=TakeFirst())
    Email = scrapy.Field(output_processor=TakeFirst())
    Vehicles_Owned_Percent = scrapy.Field(output_processor=Join())
    Vehicles_Leased_Percent = scrapy.Field(output_processor=TakeFirst())
    li_property = scrapy.Field(input_processor=MapCompose(clean), output_processor=Join())
    li_passenger = scrapy.Field(input_processor=MapCompose(clean), output_processor=Join())
    li_goods = scrapy.Field(input_processor=MapCompose(clean), output_processor=Join())
    li_broker = scrapy.Field(input_processor=MapCompose(clean), output_processor=Join())
    Insurance = scrapy.Field(input_processor=MapCompose(clean))

    owned_breakdown = scrapy.Field()
    term_breakdown = scrapy.Field()
    trip_breakdown = scrapy.Field()
    pass
