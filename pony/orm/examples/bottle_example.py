from __future__ import absolute_import, print_function

from bottle import default_app, install, route, request, redirect, run, template

# Import eStore model http://editor.ponyorm.com/user/pony/eStore
from pony.orm.examples.estore import *
from pony.orm.integration.bottle_plugin import PonyPlugin

# After the plugin is installed each request will be processed
# in a separate database session. Once the HTTP request processing
# is finished the plugin does the following:
#  * commit the changes to the database (or rollback if an exception happened)
#  * clear the transaction cache
#  * return the database connection to the connection pool
install(PonyPlugin())

@route('/')
@route('/products/')
def all_products():
    # Get the list of all products from the database
    products = select(p for p in Product)
    return template('''
    <h1>List of products</h1>
    <ul>
    %for p in products:
        <li><a href="/products/{{ p.id }}/">{{ p.name }}</a>
    %end
    </ul>
    ''', products=products)

@route('/products/:id/')
def show_product(id):
    # Get the instance of the Product entity by the primary key
    p = Product[id]
    # You can traverse entity relationship attributes inside the template
    # In this examples it is many-to-many relationship p.categories
    # Since the data were not loaded into the cache yet,
    # it will result in a separate SQL query.
    return template('''
    <h1>{{ p.name }}</h1>
    <p>Price: {{ p.price }}</p>
    <p>Product categories:</p>
    <ul>
    %for c in p.categories:
        <li>{{ c.name }}
    %end
    </ul>
    <a href="/products/{{ p.id }}/edit/">Edit product info</a>
    <a href="/products/">Return to all products</a>
    ''', p=p)

@route('/products/:id/edit/')
def edit_product(id):
    # Get the instance of the Product entity and display its attributes
    p = Product[id]
    return template('''
    <form action='/products/{{ p.id }}/edit/' method='post'>
      <table>
        <tr>
          <td>Product name:</td>
          <td><input type="text" name="name" value="{{ p.name }}">
        </tr>
        <tr>
          <td>Product price:</td>
          <td><input type="text" name="price" value="{{ p.price }}">
        </tr>
      </table>
      <input type="submit" value="Save!">
    </form>
    <p><a href="/products/{{ p.id }}/">Discard changes</a>
    <p><a href="/products/">Return to all products</a>
    ''', p=p)

@route('/products/:id/edit/', method='POST')
def save_product(id):
    # Get the instance of the Product entity
    p = Product[id]
    # Update the attributes with the new values
    p.name = request.forms.get('name')
    p.price = request.forms.get('price')
    # We might put the commit() command here, but it is not necessary
    # because PonyPlugin will take care of this.
    redirect("/products/%d/" % p.id)
    # The Bottle's redirect function raises the HTTPResponse exception.
    # Normally PonyPlugin closes the session with rollback
    # if a callback function raises an exception. But in this case
    # PonyPlugin understands that this exception is not the error
    # and closes the session with commit.


run(debug=True, host='localhost', port=8080, reloader=True)
