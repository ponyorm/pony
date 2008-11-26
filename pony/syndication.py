import time, rfc822

from pony.thirdparty import etree
from pony.thirdparty.etree import Element, SubElement, tostring

from pony.templating import Html, StrHtml

ns = 'http://www.w3.org/2005/Atom'
nsmap = getattr(etree, '_namespace_map', {})
nsmap.setdefault(ns, 'atom')

nsmap_keyargs = {}
if getattr(etree, 'LXML_VERSION', (0, 0, 0, 0)) >= (1, 3, 7, 0):
    nsmap_keyargs = {'nsmap': {'atom':ns}}

def utc(dt):
    if getattr(dt, 'tzinfo', None) is None: return dt
    return dt.replace(tzinfo=None) - dt.utcoffset()

def atom_date(date):
    return utc(date).strftime('%Y-%m-%dT%H:%M:%SZ')

def rss2_date(date):
    return rfc822.formatdate(time.mktime(utc(date).timetuple()))

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

def rss2_author(author):
    if isinstance(author, basestring): return author
    if len(author) >= 2: return '%s (%s)' % (author[1], author[0])
    return author[0]

def set_atom_text(element, text):
    if isinstance(text, (Html, StrHtml)):
        element.set('type', 'html')
        element.text = text[:]
    elif isinstance(text, basestring):
        element.set('type', 'text')
        element.text = text
    elif hasattr(text, 'makeelement') and getattr(text, 'tag') == '{http://www.w3.org/1999/xhtml}div':
        element.set('type', 'xhtml')
        element.append(text)
    else: raise TypeError('Inappropriate text value: %r' % text)

def set_rss2_text(element, text):
    if hasattr(text, 'makeelement'): element.text = tostring(text)
    elif isinstance(text, basestring): element.text = text[:]

class Feed(object):
    def __init__(self, title, link, updated,
                 id=None, subtitle=None, feed_link=None, author=None, rights=None,
                 base=None, language=None, icon=None, logo=None):
        self.link = link
        self.title = title
        self.updated = updated
        self.id = id or self.link
        self.subtitle = subtitle
        self.feed_link = feed_link
        self.author = author
        self.rights = rights
        self.base = base
        self.language = language
        self.icon = icon
        self.logo = logo
        self.entries = []
    def add(self, entry):
        self.entries.append(entry)
    def __str__(self):
        return tostring(self.atom())
    def atom(self, pretty_print=True):
        indent = pretty_print and '\n  ' or ''

        feed = Element('{http://www.w3.org/2005/Atom}feed', **nsmap_keyargs)
        feed.text = indent
        if self.base: feed.set('base', self.base)
        if self.language: feed.set('lang', self.language)

        title = SubElement(feed, '{http://www.w3.org/2005/Atom}title')
        set_atom_text(title, self.title)

        if self.subtitle:
            subtitle = SubElement(feed, '{http://www.w3.org/2005/Atom}summary')
            set_atom_text(subtitle, self.subtitle)

        link = SubElement(feed, '{http://www.w3.org/2005/Atom}link', href=self.link)

        if self.feed_link:
            feed_link = SubElement(feed, '{http://www.w3.org/2005/Atom}link', rel='self', href=self.feed_link)

        updated = SubElement(feed, '{http://www.w3.org/2005/Atom}updated')
        updated.text = atom_date(self.updated)

        id = SubElement(feed, '{http://www.w3.org/2005/Atom}id')
        id.text = self.id

        if self.author:
            author = atom_author(self.author)
            feed.append(author)

        if self.rights:
            rights = SubElement(feed, '{http://www.w3.org/2005/Atom}rights')
            set_atom_text(rights, self.rights)

        if self.icon:
            icon = SubElement(feed, '{http://www.w3.org/2005/Atom}icon')
            icon.text = self.icon

        if self.logo:
            logo = SubElement(feed, '{http://www.w3.org/2005/Atom}logo')
            logo.text = self.logo

        for entry in self.entries:
            entry = entry.atom(pretty_print)
            entry[-1].tail = indent
            feed.append(entry)

        for child in feed: child.tail = indent   
        feed[-1].tail = pretty_print and '\n' or ''
        return feed
    def rss2(self, pretty_print=True):
        indent = pretty_print and '\n    ' or ''
        indent2 = pretty_print and '\n      ' or ''

        rss = Element('rss', version='2.0')
        rss.text = pretty_print and '\n  ' or ''

        channel = SubElement(rss, 'channel')
        channel.text = indent
        channel.tail = pretty_print and '\n' or ''

        set_rss2_text(SubElement(channel, 'title'), self.title)
        set_rss2_text(SubElement(channel, 'description'), self.subtitle or '')
        SubElement(channel, 'link').text = self.link
        SubElement(channel, 'lastBuildDate').text = rss2_date(self.updated)
        if self.language: SubElement(channel, 'language').text = self.language
        if self.rights: SubElement(channel, 'copyright').text = self.rights
        if self.logo:
            image = SubElement(channel, 'image')
            image.text = indent2
            SubElement(image, 'url').text = self.logo
            SubElement(image, 'title').text = ''
            SubElement(image, 'link').text = self.link
            for child in image: child.tail = indent2
            image[-1].tail = pretty_print and '\n    ' or ''

        for entry in self.entries:
            item = entry.rss2(pretty_print)
            item[-1].tail = indent
            channel.append(item)

        for child in channel: child.tail = indent
        channel[-1].tail = pretty_print and '\n  ' or ''
        return rss

class Entry(object):
    def __init__(self, title, link, updated,
                 id=None, summary=None, content=None, published=None,
                 enclosure=None, author=None, rights=None, base=None, language=None):
        self.link = link
        self.title = title
        self.updated = updated
        self.id = id or self.link
        self.summary = summary
        self.content = content
        self.published = published
        self.enclosure = enclosure
        self.author = author
        self.rights = rights
        self.base = base
        self.language = language
    def __str__(self):
        return tostring(self.atom())
    def atom(self, pretty_print=True):
        indent = pretty_print and '\n    ' or ''

        entry = Element('{http://www.w3.org/2005/Atom}entry', **nsmap_keyargs)
        entry.text = indent
        if self.base: entry.set('base', self.base)
        if self.language: entry.set('lang', self.language)

        link = SubElement(entry, '{http://www.w3.org/2005/Atom}link', href=self.link)

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
            href, media_type, length = self.enclosure
            enclosure = SubElement(entry, '{http://www.w3.org/2005/Atom}link',
                                   rel='enclosure', href=href, type=media_type, length=length)

        if self.author:
            author = atom_author(self.author)
            entry.append(author)

        if self.rights:
            rights = SubElement(entry, '{http://www.w3.org/2005/Atom}rights')
            set_atom_text(rights, self.rights)

        if self.published:
            published = SubElement(entry, '{http://www.w3.org/2005/Atom}published')
            published.text = atom_date(self.published)

        for child in entry: child.tail = indent
        entry[-1].tail = pretty_print and '\n' or ''
        return entry
    def rss2(self, pretty_print=True):
        indent = pretty_print and '\n      ' or ''
        item = Element('item')
        item.text = indent
        set_rss2_text(SubElement(item, 'title'), self.title)
        set_rss2_text(SubElement(item, 'description'), self.summary or self.content)
        SubElement(item, 'link').text = self.link
        SubElement(item, 'guid', isPermaLink=(self.id == self.link and 'true' or 'false')).text = self.id
        if self.enclosure:
            href, media_type, length = self.enclosure
            SubElement(item, 'enclosure', url=href, type=media_type, length=length)
        if self.author: SubElement(item, 'author').text = rss2_author(self.author)
        if self.published: SubElement(item, 'pubDate').text = rss2_date(self.published)
        for child in item: child.tail = indent
        item[-1].tail = pretty_print and '\n' or ''
        return item
