
@plaintext
def f1():
    name = 'John'
    print "Hello, &(name)!"
    print "This is plain text"

def f1_explained():
    result = []
    name = 'John'
    result.append(plaintext("Hello, &(name)!"))
    result.append(plaintext("This is plain text"))
    return "".join(result)

@html
def f2():
    title = 'My title'
    students = select(s for s in Student if s.group.number == 4142)
    paginator = Paginator(students)
    print "<html>"
    print "<head><title>&(title)</title></head>"
    print "<body>"
    print "<h1>&{Welcome!}</h1>"
    print paginator.html()
    for stud in paginator.current_page(): print stud.html()
    print "</body></html>"

def f3():
    title = 'My title'
    students = select(s for s in Student if s.group.number == 4142)
    paginator = Paginator(students, single_page=True)
    return html("""
    <html>
    <head><title>&(title)</title></head>
    <body>
    <h1>&{Welcome!}</h1>
    &(paginator)
    &for(stud in paginator.current_page()) { &(stud) }
    </body></html>
    """)  #, unindent=False)

def f4():
    title = 'My title'
    students = select(s for s in Student if s.group.number == 4142)
    paginator = Paginator(students)
    return html()
    # return html(name="myfile")
