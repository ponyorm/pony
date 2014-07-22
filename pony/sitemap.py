from __future__ import absolute_import, print_function

from xml.etree.cElementTree import Element, SubElement, tostring
from xml.etree.ElementTree import _namespace_map

ns = 'http://www.sitemaps.org/schemas/sitemap/0.9'
_namespace_map.setdefault(ns, 'sitemap')

# nsmap_keyargs = {}
# if getattr(etree, 'LXML_VERSION', (0, 0, 0, 0)) >= (1, 3, 7, 0):
#     nsmap_keyargs = {'nsmap': {'sitemap':ns}}

nstags_cache = {}

def nstag(tag):
    return nstags_cache.get(tag) or nstags_cache.setdefault(tag, '{%s}%s' % (ns, tag))

def generate_sitemap(links):
    urlset = Element(nstag('urlset')) #, **nsmap_keyargs) # lxml
    urlset.text = '\n'
    for link in links:
        if isinstance(link, basestring):
            loc = link
            lastmod = changefreq = priority = None
        else: loc, lastmod, changefreq, priority = (tuple(link) + (None, None, None))[:4]
        url = SubElement(urlset, nstag('url'))
        url.text = url.tail = '\n'
        SubElement(url, nstag('loc')).text = loc
        if lastmod:
            if not isinstance(lastmod, basestring):
                if getattr(lastmod, 'tzinfo', None) is not None:
                    lastmod = lastmod.replace(tzinfo=None) - lastmod.utcoffset()
                lastmod = lastmod.strftime('%Y-%m-%dT%H:%M:%SZ')
            SubElement(url, nstag('lastmod')).text = lastmod
        if changefreq:
            SubElement(url, nstag('changefreq')).text = changefreq
        if priority is not None:
            SubElement(url, nstag('priority')).text = str(priority)
        for child in url: child.tail = '\n'
    return urlset
