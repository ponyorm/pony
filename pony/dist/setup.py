from distutils.core import setup
import sys

python_versions = ['2.4', '2.5']

name = "Pony"
version = "0.1"
desc = "web framework"
long_desc = "Pony is a web framework"
classifiers=[
    "Development Status :: 3 - Alpha",
    "Environment :: Web Environment",
    "Intended Audience :: Developers",
    "License :: AGPL",
    "Operating System :: OS Independent",
    "Programming Language :: Python",
    "Topic :: Internet :: WWW/HTTP :: Dynamic Content :: ORM",
    "Topic :: Software Development :: Libraries :: Web Frameworks",
]
author="Pony Team"
author_email="team@justpony.org"
url="http://www.justpony.org"
cp_license="AGPL"
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
data_files=[]

def main():
    match = False
    for v in python_versions:
        if sys.version.startswith(v):
            match = True
            break
    if not match:
        s = "I'm sorry, but %s %s requires Python version %s. You have version %s"
        print s % (name, version, " or ".join(python_versions), sys.version.split(' ', 1)[0])
        sys.exit(1)
    
    dist = setup(
        name=name,
        version=version,
        description=desc,
        long_description=long_desc,
        classifiers=classifiers,
        author=author,
        author_email=author_email,
        url=url,
        license=cp_license,
        packages=packages,
        download_url=download_url,
        data_files=data_files,
    )

if __name__ == "__main__":
    main()
