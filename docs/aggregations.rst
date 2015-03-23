=============
Aggregation
=============

Pony allows the use of five aggregate functions in declarative queries:  ``sum``, ``count``, ``min``, ``max``, and ``avg``. Let's see some examples of simple queries using these functions.

Total GPA of students from group 101::

	sum(s.gpa for s in Student if s.group.number == 101)

Number of students with a GPA above three::

	count(s for s in Student if s.gpa > 3)

First name of a student, who studies philosophy, sorted alphabetically::

	min(s.name for s in Student if "Philosophy" in s.courses.name)

Birth date of the youngest student in group 101::

	max(s.dob for s in Student if s.group.number == 101)

Average GPA in department 44::

	avg(s.gpa for s in Student if s.group.dept.number == 44)


.. note:: Although Python already has standard functions for ``sum``, ``count``, ``min``, and ``max``, Pony adds its own functions under the same names. Also, Pony adds its own ``avg`` function. These functions are implemented in the ``pony.orm`` module and they can be imported from there either "by the star", or by the name.

  Functions implemented in Pony expand the behavior of standard functions in Python; thus, if in a program these functions are used in their standard designation, the import will not affect their behavior. But it also allows you to specify a declarative query inside the function.

  If one forgets to import, then an error will appear upon use of the Python standard functions ``sum``, ``count``, ``min``, and ``max`` with a declarative query as a parameter:
  
  .. code-block:: python

        TypeError: Use a declarative query in order to iterate over entity

Aggregate functions can also be used inside a query. For example, if we need to find not only the birth date of the youngest student in the group, but also the student himself::

    select(s for s in Student 
           if s.group.number == 101 
           and s.dob == max(s.dob for s in Student 
                            if s.group.number == 101))

Or, for example, to list all groups with an average GPA above 4.5:: 

	select(g for g in Group if avg(s.gpa for s in g.students) > 4.5)

This query can be shorter if we use Pony's attribute propagation feature::

	select(g for g in Group if avg(g.students.gpa) > 4.5)



Several aggregate functions in one query
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

SQL allows you to include several aggregate functions in the same query. For example, we might want to receive both the lowest and the highest GPA for each group. In SQL, such a query would look like this:

.. code-block:: sql

    SELECT s.group_number, MIN(s.gpa), MAX(s.gpa)
    FROM Student s
    GROUP BY s.group_number

This request will return the lowest and the highest GPA for each group. Pony allows you to use the same approach::

	select((s.group, min(s.gpa), max(s.gpa)) for s in Student)


Grouping 
~~~~~~~~~~~~

Queries in Pony look shorter than a similar queries in SQL, since in SQL we have to indicate a “GROUP BY” section. Pony, on the other hand, understands the need for this section and includes it automatically. How does Pony do it? 

One might note that the GROUP BY SQL query section includes the columns, which are also included in the SELECT section, and are not included in aggregative functions. That is, it is necessary to duplicate the list of these columns in SQL. Pony avoids this duplication, as it understands that if an expression is included in a query result and is not included in the aggregative function, it should be added to the GROUP BY section.  


Function "count" 
~~~~~~~~~~~~~~~~~~

Aggregative queries often request to calculate the quantity of something, and in Pony this request is served by the function ``count``. For example, we want to count the number of students in Group 101::

	count(s for s in Student if s.group.number == 101)

Or the number of students in each group related to 44th Department::

	select((g, count(g.students)) for g in Group if g.dept.number == 44)

or::

	select((s.group, count(s)) for s in Student if s.group.dept.number == 44)

In the first example the aggregate function ``count`` receives a collection, and Pony will translate it into a subquery (although Pony will try to optimize the query and replace it with ``LEFT JOIN``).

In the second example, the function ``count`` receives a single object instead of a collection. In this case Pony will add a ``GROUP BY`` section to the SQL query and the grouping will be done on the ``s.group`` attribute.

If you use the ``count()`` function without arguments, this will be translated to ``COUNT(*)``. If you specify an argument, it will be translated to ``COUNT(DISTINCT column)``


Conditional COUNT
~~~~~~~~~~~~~~~~~~~~

There is also another way to use the COUNT function. Let's assume that we want to calculate three numbers for each group - the number of students that have a GPA less than 3, the number of students with GPA between 3 to 4, and the number of students with GPA higher than 4. A traditional expression of this query would be cumbersome::

	select((g, count(s for s in g.students if s.gpa <= 3), 
                count(s for s in g.students if s.gpa > 3 and s.gpa <= 4), 
                count(s for s in g.students if s.gpa > 4)) for g in Group)

not only would this query be pretty long, but it would also be very ineffective, as it will execute each COUNT as a separate subquery. For these cases, Pony has a "conditional COUNT" syntax::

	select((s.group, count(s.gpa <= 3), 
            count(s.gpa > 3 and s.gpa <= 4), 
            count(s.gpa > 4)) for s in Student)

This way, we indicate in the ``count`` function a certain condition, rather than a column; and ``count`` calculates the number of objects for which this condition is true. This query will not include subqueries which makes it more effective.

.. note:: The queries above are not entirely equivalent: if a group doesn't have any students, then the first query will select that group having zeros as the result of ``count``, while the second query simply will not select the group at all. This happens because the second query selects the rows from the table Student, and if the group doesn't have any students, then the table Student will not have any rows for this group.

  If we want to get rows with zeros, then an effective SQL query should use LEFT JOIN::

	left_join((g, count(s.gpa <= 3), 
               count(s.gpa > 3 and s.gpa <= 4), 
               count(s.gpa > 4)) for g in Group for s in g.students)



More complex grouping options
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Pony allows you to carry out grouping not only by object attributes, but also by more complex expressions – such as grouping a set of students by birth year and calculating their average GPA. Birth year in this case is not a distinct attribute – it is a part of the ``dob`` attribute. Therefore, the query will look like this::  

	select((s.dob.year, avg(s.gpa)) for s in Student)

Similarly, inside aggregate functions you can indicate not only simple attributes, but also expressions. For example, let’s suppose we have the database of an internet retailer, which contains information regarding goods and orders::

	select((item.order, sum(item.price * item.quantity)) 
            for item in OrderItem if item.order.id == 123)

or like this::

	select((order, sum(order.items.price * order.items.quantity)) 
            for order in Order if order.id == 123)

In the second case, we make use of the attribute propagation concept, and the expression ``order.items.price`` creates an array of prices, while ``order.items.quantity`` generates an array of quantities. It might appear, that these two arrays are impossible to multiply correctly, as they are not just numbers, but Pony can "do the right thing" and multiply the quantity by the price for each order item.  

As usual, the option with attribute propagation is translated to the subquery, while the first option is translated to the more effective grouping query.



Queries with HAVING
~~~~~~~~~~~~~~~~~~~~~~

SELECT construction in SQL includes two sections, conditions can be written using WHERE and HAVING. The WHERE component includes those conditions that are applied before the grouping, while the HAVING conditions are applied after the grouping. In Pony, all conditions in a query are written after ``if``. If Pony recognizes the use of an aggregative function that includes a single condition, rather than a subquery, it understands that this condition should be applied to the HAVING section.

Let's assume that we want to write a query that lists the groups of 44th Department with average GPAs higher than 4.0, and the number of students in every such group. This type of query can be written like:: 

	select((s.group, count(s)) for s in Student 
             if s.group.dept.number == 44 and avg(s.gpa) > 4)

In this query, the ``if`` section includes two conditions. The first condition ``s.group.dept.number = 44`` is not included in the aggregative functions and therefore it will be used as a WHERE condition; while the second condition ``avg(s.gpa) > 4`` is passed to the ``avg`` function, and thus will be used as a HAVING condition. 

.. note:: It is assumed that conditions are separated by a logical ``and``; that is, if conditions are separated by ``or``, then Pony will assume it is a single condition and put it entirely in the HAVING section.

If a condition intended for the HAVING section has an expression located outside the limits of the aggregate functions, and at the same time is not contained by GROUP BY, it will be added to GROUP BY.

For example, if we want to find orders where the total order price differs from the sum of the order items (due to a discount), our query would look like this::

	select((item.order, item.order.total_price, 
             sum(item.price * item.quantity)) 
             for item in OrderItem 
             if item.order.total_price < sum(item.price * item.quantity))

In this query, the condition contains an aggregate function; this is why it will go into the HAVING section. The GROUP BY section will contain ``item.order``, because it is indicated in the list of criteria. But besides this, the GROUP BY section will contain ``item.order.total_price``, because this attribute falls into the HAVING section and is not inside the aggregate function. Why is this attribute added? Because without it, a DBMS such as Oracle, PostgreSQL and Microsoft SQL Server will refuse to execute the query, and will return a syntax error. In any case, this is a standard requirement of SQL.


In this case, an order has a single value ``total_price``, therefore adding a column will not affect the result.  It is importart to be careful here, since additing extra columns might lead to grouping based on smaller row groups which could affect the result of the query. In general, creating queries with conditions in the HAVING section requires high level of professionalism.


Query optimization
~~~~~~~~~~~~~~~~~~~~

When the speed of a query is an important factor, we can construct the query in such a way that will force the database to use grouping instead of subqueries. However, finding a way to create such an optimized query is not always simple. In such cases, we can use the special function called ``JOIN``, which serves as a hint to Pony that we want to use JOIN, instead of generating subqueries. 

Let's assume that we need to create the following query: find all groups, where the highest GPA is lower than 4.0. We can use this expression::

    select(g for g in Group if max(g.students.gpa) < 4)

If we do not want to search by way of subqueries, we can simply wrap an expression in a JOIN function, which should join the relevant tables::

    select(g for g in Group if JOIN(max(g.students.gpa) < 4))

We used JOIN to wrap the whole condition, and received an optimized query as an outcome.

In this specific case, we could try to find a formulation, which would make an effective request without using the JOIN function::

    select(s.group for s in Student if max(s.gpa) < 4)

However, if a declarative expression includes more than one query, we will not be able to find better formulation for the query, and we will have to use the ``JOIN`` hint.


Aggregation queries with sorting
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Let’s suppose that we want, for each group, to calculate average GPA and sort the results so that groups with the lowest average GPA appear first. We can accomplish the sorting like this::

	select((s.group, avg(s.gpa)) for s in Student) \
            .order_by(lambda: avg(s.gpa))

If we want to sort the rows in reverse order, we can use the ``desc()`` function::

	select((s.group, avg(s.gpa)) for s in Student) \
            .order_by(lambda: desc(avg(s.gpa)))

In other words, from the received query we call up the order_by method, passing it a lambda function without parameters, in which we parenthetically list the sorting expressions, using the same variable names as in the actual query.

Another variation, simpler but also more limited, would be to parenthetically indicate in the order_by method the number of the column (starting with 1), that we wish to sort by. A negative number indicates sorting by descending. Using this method, the two queries mentioned above will look like this:

Ascending by the second column::

	select((s.group, avg(s.gpa)) for s in Student).order_by(2)

descending by the second column::

	select((s.group, avg(s.gpa)) for s in Student).order_by(-2)

If necessary, aggregative functions can be listed as methods. That is, there are two possible options::

	select(sum(s.gpa) for s in Student)
	select(s.gpa for s in Student).sum()

and so on for other functions.