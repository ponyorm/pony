import unittest
from typing import TYPE_CHECKING

from pony.orm.core import *
from pony.orm.tests import db_params, teardown_database

db = Database()

if TYPE_CHECKING:
    from pony.orm.core import Entity
else:
    Entity = db.Entity


class Author(Entity):
    books = Set("Book")
    reviews = Set("Review", cascade_delete=False)


class Book(Entity):
    authors = Set(Author)


class Review(Entity):
    author = Required(Author)


class TestSetUndoFunctions(unittest.TestCase):
    """A failure after changing a many-to-many collection must be undone

    The undo functions restore each object depending on its own state, and
    the state differs between the objects: one link is already stored in the
    database, the other one is not. They must not fail, otherwise they hide
    the original exception.
    """

    @classmethod
    def setUpClass(cls):
        db.bind(**db_params)
        db.generate_mapping(check_tables=False)
        db.drop_all_tables(with_all_data=True)

    def setUp(self):
        db.create_tables()
        with db_session:
            author = Author()
            linked_book = Book()
            other_book = Book()
            author.books.add(linked_book)
            Review(author=author)
            flush()
            self.author_id = author.id
            self.linked_book_id = linked_book.id
            self.other_book_id = other_book.id

    def tearDown(self):
        teardown_database(db)

    def test_failed_delete(self):
        # the undo function of reverse_remove: the link to the new book was not
        # stored yet (it is in 'added'), the link to the linked book was
        with db_session:
            author = Author[self.author_id]
            linked_book = Book[self.linked_book_id]
            new_book = Book()
            author.books.add(new_book)
            with self.assertRaises(ConstraintError):
                author.delete()

            linked = linked_book._vals_[Book.authors]
            self.assertIn(author, linked)
            self.assertFalse(linked.added)
            self.assertFalse(linked.removed)
            new = new_book._vals_[Book.authors]
            self.assertIn(author, new)
            self.assertEqual(new.added, {author})
            self.assertFalse(new.removed)
            rollback()

    def test_failed_set(self):
        # the undo function of reverse_add: the link to the linked book was
        # removed in this transaction (it is in 'removed'), the other book
        # has never been linked
        with db_session:
            author = Author[self.author_id]
            linked_book = Book[self.linked_book_id]
            other_book = Book[self.other_book_id]
            author.books.remove(linked_book)
            with self.assertRaises(ValueError):
                author.set(books=[linked_book, other_book], reviews=[])

            linked = linked_book._vals_[Book.authors]
            self.assertNotIn(author, linked)
            self.assertFalse(linked.added)
            self.assertEqual(linked.removed, {author})
            other = other_book._vals_[Book.authors]
            self.assertNotIn(author, other)
            self.assertFalse(other.added)
            self.assertFalse(other.removed)
            rollback()


if __name__ == "__main__":
    unittest.main()
