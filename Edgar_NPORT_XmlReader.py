import csv
import requests
import xml.etree.ElementTree as ET
import xml.dom.minidom as MD

# url of the EDGAR filing
# this will need to be edited for each filing you which to parse
# url = 'https://www.sec.gov/Archives/edgar/data/1268533/000114554921044394/primary_doc.xml'
url = 'https://www.sec.gov/Archives/edgar/data/102426/000175272424093409/xslFormNPORT-P_X01/primary_doc.xml'

file_name = 'STEW_MonthlyPortfolio.xml'
xmlfile = fr'G:/Downloads/{file_name}'
# xmlfile = file_name


# def getXML_url():
#
#     # url of the EDGAR filing
#     resp = requests.get(url)
#
#
def parseXML():

    # create element tree object6
    tree = ET.parse(xmlfile)

    # get root element
    root = tree.getroot()

    # create empty lis for data
    data = []
#
#     # iterate data
#     for item in root.


def main():
    # load the url from the web
    # getXML_url()

    parseXML()

    # parse the xml file
    # parseXML()

# def main():
#     doc = MD.parse
#
#     print(doc.nodeName)
#     print(doc.firstChild.tagName)


if __name__=="__main__":
    #call the main function
    main()