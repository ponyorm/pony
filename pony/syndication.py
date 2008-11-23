from pony.thirdparty import etree, rfc3339
from pony.thirdparty.etree import Element, SubElement, tostring

from pony.templating import Html, StrHtml

ns = 'http://www.w3.org/2005/Atom'
nsmap = getattr(etree, '_namespace_map', {})
nsmap.setdefault(ns, 'atom')

nsmap_keyargs = {}
if getattr(etree, 'LXML_VERSION', (0, 0, 0, 0)) >= (1, 3, 7, 0):
    nsmap_keyargs = {'nsmap': {'atom':ns}}

def atom_date(date):
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
    def __str__(self):
        return tostring(self.atom())
    def atom(self, pretty_print=True):
        indent = pretty_print and '\n  ' or ''
        feed = Element('{http://www.w3.org/2005/Atom}feed', **nsmap_keyargs)
        feed.text = indent
        link = SubElement(feed, '{http://www.w3.org/2005/Atom}link')
        link.set('{http://www.w3.org/2005/Atom}href', self.link)
        link.tail = indent
        title = SubElement(feed, '{http://www.w3.org/2005/Atom}title')
        set_atom_text(title, self.title)
        title.tail = indent
        updated = SubElement(feed, '{http://www.w3.org/2005/Atom}updated')
        updated.text = atom_date(self.updated)
        updated.tail = indent
        id = SubElement(feed, '{http://www.w3.org/2005/Atom}id')
        id.text = self.id
        id.tail = indent
        if self.subtitle:
            subtitle = SubElement(feed, '{http://www.w3.org/2005/Atom}summary')
            set_atom_text(subtitle, self.subtitle)
            subtitle.tail = indent
        if self.author:
            author = atom_author(self.author)
            feed.append(author)
            author.tail = indent
        if self.feed_link:
            feed_link = SubElement(feed, '{http://www.w3.org/2005/Atom}link')
            feed_link.set('{http://www.w3.org/2005/Atom}rel', 'self')
            feed_link.set('{http://www.w3.org/2005/Atom}href', self.feed_link)
            feed_link.tail = indent
        if self.rights:
            rights = SubElement(feed, '{http://www.w3.org/2005/Atom}rights')
            set_atom_text(rights, self.rights)
            rights.tail = indent
        for entry in self.entries:
            entry = entry.atom(pretty_print)
            entry[-1].tail = indent
            feed.append(entry)
            entry.tail = indent
        feed[-1].tail = pretty_print and '\n' or ''
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
    def __str__(self):
        return tostring(self.atom())
    def atom(self, pretty_print=True):
        indent = pretty_print and '\n    ' or ''
        entry = Element('{http://www.w3.org/2005/Atom}entry', **nsmap_keyargs)
        entry.text = indent
        link = SubElement(entry, '{http://www.w3.org/2005/Atom}link')
        link.set('{http://www.w3.org/2005/Atom}href', self.link)
        link.tail = indent
        title = SubElement(entry, '{http://www.w3.org/2005/Atom}title')
        set_atom_text(title, self.title)
        title.tail = indent
        updated = SubElement(entry, '{http://www.w3.org/2005/Atom}updated')
        updated.text = atom_date(self.updated)
        updated.tail = indent
        id = SubElement(entry, '{http://www.w3.org/2005/Atom}id')
        id.text = self.id
        id.tail = indent
        if self.summary:
            summary = SubElement(entry, '{http://www.w3.org/2005/Atom}summary')
            set_atom_text(summary, self.summary)
            summary.tail = indent
        if self.content:
            content = SubElement(entry, '{http://www.w3.org/2005/Atom}content')
            set_atom_text(content, self.content)
            content.tail = indent
        if self.enclosure:
            enclosure = SubElement(entry, '{http://www.w3.org/2005/Atom}link')
            enclosure.set('{http://www.w3.org/2005/Atom}rel', 'enclosure')
            enclosure.set('{http://www.w3.org/2005/Atom}href', self.enclosure)
            enclosure.tail = indent
        if self.author:
            author = atom_author(self.author)
            entry.append(author)
            author.tail = indent
        if self.rights:
            rights = SubElement(entry, '{http://www.w3.org/2005/Atom}rights')
            set_atom_text(rights, self.rights)
            rights.tail = indent
        if self.published:
            published = SubElement(entry, '{http://www.w3.org/2005/Atom}published')
            published.text = atom_date(self.published)
            published.tail = indent
        entry[-1].tail = pretty_print and '\n' or ''
        return entry
