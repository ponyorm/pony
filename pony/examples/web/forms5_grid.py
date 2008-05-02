from pony.main import *

use_autoreload()

@webpage
def grid1():
    u"Simple grid example"
    f = Form(method='GET')
    f.text = Text()
    f.grid = Grid(columns=['First', 'Second', 'Third'], row_count=2)
    print f

@webpage
def grid2():
    u"Grid with different field types"
    f = Form(method='GET')
    f.grid = Grid(columns=['First', 'Second', 'Third'])
    f.grid.row_count = 3
    f.grid[0, 0] = StaticText(value='Static text!')
    f.grid[0, 1] = Select(options=['red', 'green', 'blue'])
    f.grid[0, 2] = Checkbox(value=True)
    f.grid[1, 0] = Text(value='AAA')
    f.grid[1, 1] = None
    f.grid[1, 2] = Text(value='BBB')
    f.grid[2, 0] = StaticText(value='Static text!')
    f.grid[2, 1] = Text(value='CCC')
    f.grid[2, 2] = StaticText(value='Static text!')
    print f

@webpage('/')
def index():
    print '<p>%s</p>' % link(grid1)
    print '<p>%s</p>' % link(grid2)
    
http.start()
