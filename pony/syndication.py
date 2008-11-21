from pony.thirdparty import etree, rfc3339
from pony.thirdparty.etree import Element, SubElement, tostring

from pony.templating import Html, StrHtml

ns = 'http://www.w3.org/2005/Atom'
nsmap = getattr(etree, '_namespace_map', {})
nsmap.setdefault(ns, 'atom')

def set_atom_text(element, text):
    if isinstance(text, (Html, StrHtml)):
        element.set('{http://www.w3.org/2005/Atom}type', 'html')
        element.text = text[:]
    elif isinstance(text, basestring):
        element.set('{http://www.w3.org/2005/Atom}type', 'text')
        element.text = text
    elif hasattr(text, 'makeelement') and getattr(text, 'tag') == '{http://www.w3.org/1999/xhtml}div':
        element.set('{http://www.w3.org/2005/Atom}type', 'xhtml')
        element.append(text)
    else: raise TypeError('Inappropriate text value: %r' % text)

def atom_date(date):
    if isinstance(date, basestring): return date
    return rfc3339.rfc3339(date)

def atom_author(author):
    if isinstance(author, basestring): author = [ author, None, None ]
    else:
        try: iter(author)
        except TypeError: raise TypeError('Inappropriate author value: %r' % author)
        author = (list(author) + [ None, None ])[:3]
    name, uri, email = author
    result = Element('{http://www.w3.org/2005/Atom}author')
    if name: SubElement(result, '{http://www.w3.org/2005/Atom}name').text = name
    if uri: SubElement(result, '{http://www.w3.org/2005/Atom}uri').text = uri
    if email: SubElement(result, '{http://www.w3.org/2005/Atom}email').text = email
    return result

class Feed(object):
    def __init__(self, link, title, updated,
                 id=None, subtitle=None, feed_link=None, author=None, rights=None, icon=None, logo=None):
        self.link = link
        self.title = title
        self.updated = updated
        self.id = id or self.link
        self.subtitle = subtitle
        self.author = author
        self.feed_link = feed_link
        self.rights = rights
        self.entries = []
    def add(self, entry):
        self.entries.append(entry)
    def atom(self):
        if getattr(etree, 'LXML_VERSION', (0, 0, 0, 0)) >= (1, 3, 7, 0):
              feed = Element('{http://www.w3.org/2005/Atom}feed', nsmap={'atom':ns})
        else: feed = Element('{http://www.w3.org/2005/Atom}feed')
        link = SubElement(feed, '{http://www.w3.org/2005/Atom}link')
        link.set('{http://www.w3.org/2005/Atom}href', self.link)
        title = SubElement(feed, '{http://www.w3.org/2005/Atom}title')
        set_atom_text(title, self.title)
        updated = SubElement(feed, '{http://www.w3.org/2005/Atom}updated')
        updated.text = atom_date(self.updated)
        id = SubElement(feed, '{http://www.w3.org/2005/Atom}id')
        id.text = self.id
        if self.subtitle:
            subtitle = SubElement(feed, '{http://www.w3.org/2005/Atom}summary')
            set_atom_text(subtitle, self.subtitle)
        if self.author: feed.append(atom_author(self.author))
        if self.feed_link:
            feed_link = SubElement(feed, '{http://www.w3.org/2005/Atom}link')
            feed_link.set('{http://www.w3.org/2005/Atom}rel', 'self')
            feed_link.set('{http://www.w3.org/2005/Atom}href', self.feed_link)
        if self.rights:
            rights = SubElement(feed, '{http://www.w3.org/2005/Atom}rights')
            set_atom_text(rights, self.rights)
        for entry in self.entries: feed.append(entry.atom())
        return feed

class Entry(object):
    def __init__(self, link, title, updated,
                 id=None, summary=None, content=None, enclosure=None, author=None, rights=None, published=None):
        self.link = link
        self.title = title
        self.updated = updated
        self.id = id or self.link
        self.summary = summary
        self.content = content
        self.enclosure = enclosure
        self.author = author
        self.rights = rights
        self.published = published
    def atom(self):
        entry = Element('{http://www.w3.org/2005/Atom}entry')
        link = SubElement(entry, '{http://www.w3.org/2005/Atom}link')
        link.set('{http://www.w3.org/2005/Atom}href', self.link)
        title = SubElement(entry, '{http://www.w3.org/2005/Atom}title')
        set_atom_text(title, self.title)
        updated = SubElement(entry, '{http://www.w3.org/2005/Atom}updated')
        updated.text = atom_date(self.updated)
        id = SubElement(entry, '{http://www.w3.org/2005/Atom}id')
        id.text = self.id
        if self.summary:
            summary = SubElement(entry, '{http://www.w3.org/2005/Atom}summary')
            set_atom_text(summary, self.summary)
        if self.content:
            content = SubElement(entry, '{http://www.w3.org/2005/Atom}content')
            set_atom_text(content, self.content)
        if self.enclosure:
            enclosure = SubElement(entry, '{http://www.w3.org/2005/Atom}link')
            enclosure.set('{http://www.w3.org/2005/Atom}rel', 'enclosure')
            enclosure.set('{http://www.w3.org/2005/Atom}href', self.enclosure)
        if self.author: entry.append(atom_author(self.author))
        if self.rights:
            rights = SubElement(entry, '{http://www.w3.org/2005/Atom}rights')
            set_atom_text(rights, self.rights)
        if self.published:
            published = SubElement(entry, '{http://www.w3.org/2005/Atom}published')
            published.text = atom_date(self.published)
        return entry    
