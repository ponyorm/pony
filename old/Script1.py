import re
import pony

class WikiIndex(Page):
    @template
    def get(self):
        '''
        <head><title>Simple Wiki Demo</title></head>
        <body>
            <h1>Welcome in our small wiki!</h1>
            <p>Page index:
            &for(page in pages):
                <li>&href(page.name)
            &end(for)
        </body>
        '''
        pages = WikiPage.select().order_by('name')
    def dir(self, name):
        return WikiPage.find(name) or WikiPage(name)

name_pattern = re.compile('[A-Z]\w+[A-Z]\w+')

class WikiPage(Persistent, Page):
    name=string
    text=string
    def __init__(name, text):
        assert name_pattern.match(self.name)
        self.name = name
        self.text = text
    def rendered_text(self):
        return name_pattern.sub('<a href="\1">\1</a>', self.text)
    @template
    def get(self):
        '''
        <head><title>self.name</title></head>
        <body>
            &if(self.exists()):
                &(self.rendered_text())
            &else:
                <h1>This page still not exists!</h1>
                <h2>You can create it:
            &end(if)
            &form(action="edit", type="post")
                &text(self.text)
                <br>
                &button('Save')
                &button('Delete')
            &end(form)
        <body>
        '''
    def post(self, text, button):
        if button == 'Save':
            self.text = text
        elif button == 'Delete':
            self.delete()
        pony.commit()
        
if __name__ == "__main__":
    pony.run('localhost:8080', WikiIndex)
