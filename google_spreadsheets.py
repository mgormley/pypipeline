
import time
import gdata.spreadsheet.service
import getpass
import string
import sys

def PrintFeed(feed):
    for i, entry in enumerate(feed.entry):
        if isinstance(feed, gdata.spreadsheet.SpreadsheetsCellsFeed):
            print '%s %s\n' % (entry.title.text, entry.content.text)
        elif isinstance(feed, gdata.spreadsheet.SpreadsheetsListFeed):
            print '%s %s %s' % (i, entry.title.text, entry.content.text)
            # Print this row's value for each column (the custom dictionary is
            # built from the gsx: elements in the entry.) See the description of
            # gsx elements in the protocol guide.
            print 'Contents:'
            for key in entry.custom:
                print '    %s: %s' % (key, entry.custom[key].text)
            print '\n',
        else:
            print '%s %s\n' % (i, entry.title.text)

def PromptForSpreadsheet(gd_client):
    # Get the list of spreadsheets
    feed = gd_client.GetSpreadsheetsFeed()
    PrintFeed(feed)
    input = raw_input('\nSelection: ')
    return feed.entry[string.atoi(input)].id.text.rsplit('/', 1)[1]

def PromptForWorksheet(gd_client, key):
    # Get the list of worksheets
    feed = gd_client.GetWorksheetsFeed(key)
    PrintFeed(feed)
    input = raw_input('\nSelection: ')
    return feed.entry[string.atoi(input)].id.text.rsplit('/', 1)[1]

def ListGetAction(gd_client, key, wksht_id):
    # Get the list feed
    feed = gd_client.GetListFeed(key, wksht_id)
    PrintFeed(feed)
    return feed

def ListInsertAction(gd_client, key, wksht_id, row_data):
    entry = gd_client.InsertRow(row_data, key, wksht_id)
    if isinstance(entry, gdata.spreadsheet.SpreadsheetsList):
        print 'Inserted!'
        
def ListUpdateAction(gd_client, key, wksht_id, index, row_data):
    feed = gd_client.GetListFeed(key, wksht_id)
    entry = gd_client.UpdateRow(feed.entry[string.atoi(index)], row_data)
    if isinstance(entry, gdata.spreadsheet.SpreadsheetsList):
        print 'Updated!'
        
def ListDeleteAction(gd_client, key, wksht_id, index):
    feed = gd_client.GetListFeed(key, wksht_id)
    gd_client.DeleteRow(feed.entry[string.atoi(index)])
    print 'Deleted!'

def CellsGetAction(gd_client, key, wksht_id):
    # Get the feed of cells
    feed = gd_client.GetCellsFeed(key, wksht_id)
    PrintFeed(feed)
    
def CellsUpdateAction(gd_client, key, wksht_id, row, col, inputValue):
    entry = gd_client.UpdateCell(row=row, col=col, inputValue=inputValue, 
            key=key, wksht_id=wksht_id)
    if isinstance(entry, gdata.spreadsheet.SpreadsheetsCell):
        print 'Updated!'

def get_spreadsheet_by_title(gd_client, title):
    # Get the list of spreadsheets
    feed = gd_client.GetSpreadsheetsFeed()
    for i, entry in enumerate(feed.entry):
        if entry.title.text == title:
            return feed.entry[i].id.text.rsplit('/', 1)[1]
    return None

def get_first_worksheet(gd_client, key):
    feed = gd_client.GetWorksheetsFeed(key)
    return feed.entry[0].id.text.rsplit('/', 1)[1]

def clear_worksheet(gd_client, key, wksht_id):
    feed = gd_client.GetListFeed(key, wksht_id)
    for i, _ in enumerate(feed.entry):
        # This actually removes the rows, we just want to clear them: 
        gd_client.DeleteRow(feed.entry[i])
        entry = gd_client.UpdateRow(feed.entry[i], {})
        if not isinstance(entry, gdata.spreadsheet.SpreadsheetsList):
            sys.stderr.write("Error trying to clear row")
