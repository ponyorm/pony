from pony.thirdparty import etree
from pony.thirdparty.etree import Element, SubElement, tostring

ns = 'http://www.sitemaps.org/schemas/sitemap/0.9'
nsmap = getattr(etree, '_namespace_map', {})
nsmap.setdefault(ns, 'sitemap')

nsmap_keyargs = {}
if getattr(etree, 'LXML_VERSION', (0, 0, 0, 0)) >= (1, 3, 7, 0):
    nsmap_keyargs = {'nsmap': {'sitemap':ns}}

def generate_sitemap(links):
    urlset = Element('{http://www.sitemaps.org/schemas/sitemap/0.9}urlset', **nsmap_keyargs)
    urlset.text = '\n'
    for link in links:
        if isinstance(link, basestring):
            loc = link
            lastmod = changefreq = priority = None
        else: loc, lastmod, changefreq, priority = (tuple(link) + (None, None, None))[:4]
        url = SubElement(urlset, '{http://www.sitemaps.org/schemas/sitemap/0.9}url')
        url.text = url.tail = '\n'
        SubElement(url, '{http://www.sitemaps.org/schemas/sitemap/0.9}loc').text = loc
        if lastmod:
            if not isinstance(lastmod, basestring):
                if getattr(lastmod, 'tzinfo', None) is not None:
                    lastmod = lastmod.replace(tzinfo=None) - lastmod.utcoffset()
                lastmod = lastmod.strftime('%Y-%m-%dT%H:%M:%SZ')
            SubElement(url, '{http://www.sitemaps.org/schemas/sitemap/0.9}lastmod').text = lastmod
        if changefreq:
            SubElement(url, '{http://www.sitemaps.org/schemas/sitemap/0.9}changefreq').text = changefreq
        if priority is not None:
            SubElement(url, '{http://www.sitemaps.org/schemas/sitemap/0.9}priority').text = str(priority)
        for child in url: child.tail = '\n'
    return urlset
