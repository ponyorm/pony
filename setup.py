from distutils.core import setup
from distutils.command.install import INSTALL_SCHEMES
from glob import glob

name = "Pony"
pony_version = "0.1"
description = "web framework"
long_description = "Pony is a web framework"
classifiers=[
    "Development Status :: 3 - Alpha",
    "Environment :: Web Environment",
    "Intended Audience :: Developers",
    "License :: AGPL",
    "License :: Commercial",
    "Operating System :: OS Independent",
    "Programming Language :: Python",
    "Topic :: Internet :: WWW/HTTP :: Dynamic Content :: ORM",
    "Topic :: Software Development :: Libraries :: Web Frameworks",
]
author="Pony Team"
author_email="team@justpony.org"
url="http://www.justpony.org"
license="AGPL, Commercial"
packages=[
    "pony", "pony.captcha",
    "pony.dbproviders", 
    "pony.examples", "pony.examples.orm",
    "pony.examples.sql_ast", "pony.examples.web",
    "pony.gui", "pony.layouts",
    "pony.patches", "pony.python",
    "pony.sessionstorage", "pony.tests",
    "pony.text", "pony.thirdparty",
    "pony.thirdparty.cherrypy", "pony.thirdparty.simplejson",
    "pony.thirdparty.wsgiref" 
]
download_url="http://download.justpony.org/"
data_files = [ ('pony', ['pony/htmltb.format_exc.html', 'pony/translations.txt']), 
               ('pony/captcha', ['pony/captcha/map.dat', 'pony/captcha/VeraSe.ttf']),
               ('pony/doc', glob('pony/doc/*.txt') ),               
               ('pony/examples/orm', ['pony/examples/orm/mapping1.xml']),
               ('pony/examples/web', glob('pony/examples/web/*.html')),
               ('pony/static', glob('pony/static/*.*')),
               ('pony/static/blueprint', glob('pony/static/blueprint/*.*')),
               ('pony/static/css', glob('pony/static/css/*.*')),
               ('pony/static/img', glob('pony/static/img/*.*')),
               ('pony/static/jquery', glob('pony/static/jquery/*.*')),
               ('pony/static/js', glob('pony/static/js/*.*')), 
               ('pony/static/blueprint/src', glob('pony/static/blueprint/src/*.*')),
               ('pony/static/css/layouts', glob('pony/static/css/layouts/*.*')),
               ('pony/static/img/doc', glob('pony/static/img/doc/*.*')),
               ('pony/static/yui/reset-fonts-grids', glob('pony/static/yui/reset-fonts-grids/*.*')),
               ('pony/static/blueprint/plugins/buttons', glob('pony/static/blueprint/plugins/buttons/*.*')),
               ('pony/static/blueprint/plugins/css-classes', glob('pony/static/blueprint/plugins/css-classes/*.*')),
               ('pony/static/blueprint/plugins/fancy-type', glob('pony/static/blueprint/plugins/fancy-type/*.*')),
               ('pony/static/blueprint/plugins/link-icons', glob('pony/static/blueprint/plugins/link-icons/*.*')),
               ('pony/static/blueprint/plugins/rtl', glob('pony/static/blueprint/plugins/rtl/*.*')), 
               ('pony/static/blueprint/plugins/buttons/icons', glob('pony/static/blueprint/plugins/buttons/icons/*.*')),
               ('pony/static/blueprint/plugins/link-icons/icons', glob('pony/static/blueprint/plugins/link-icons/icons/*.*')),
               ('pony/patches', glob('pony/patches/*.*')), 
               ('pony/patches/pywin', glob('pony/patches/pywin/*.*')), 
               ('pony/patches/pywin/framework', ['pony/patches/pywin/framework/interact.py']),
               ('pony/patches/pywin/scintilla', ['pony/patches/pywin/scintilla/view.py']),
               ('pony/layouts', glob('pony/layouts/*.txt')),
               ('pony/text', glob('pony/text/*.txt'))
             ]
 

for scheme in INSTALL_SCHEMES.values():
    scheme['data'] = scheme['purelib']


from sys import version, exit
def main():
    python_version = version
    if python_version < '2.4' or python_version >= '2.7':
        s = "I'm sorry, but %s %s requires Python version 2.4, 2.5 or 2.6. You have version %s"
        print s % (name, pony_version, python_version.split(' ', 1)[0])
        exit(1)
    
    setup(
        name=name,
        version=pony_version,
        description=description,
        long_description=long_description,
        classifiers=classifiers,
        author=author,
        author_email=author_email,
        url=url,
        license=license,
        packages=packages,
        download_url=download_url,
        data_files=data_files
    )

if __name__ == "__main__":
    main()
