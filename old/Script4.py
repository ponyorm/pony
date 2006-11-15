from datetime import datetime
import pony.main

_database_ = pony.database('sqlite', 'c:\\mydb')

@url('')
def index():
    return pony.template("""
    &(std_header(title='Interesting blogs'))
    <h1>Blog list:</h1>
    &for(blog in Blog):
        &(url())
    &end(for)
    &(std_footer())    
    """)

class Blog(Persistent):
    author_name = PrimaryKey(str)
    title = Required(unicode)
    posts = Set('Post', order='Post.creation_time')

class Post(Persistent):
    blog = Required(Blog)
    creation_time = Required(datetime.datetime)
    title = Required(unicode)
    text = Required(unicode)
    keywords = Set(str)
    comments = Set('Comment', order='Comment.creation_time')
    
class Comment(Persistent):
    post = Required(Post)
    in_responce_to = Optional(Comment)
    author_name = Optional(unicode)
    author_website = Optional(str)
    author_email = Optional(str)
    title = Required(unicode)
    text = Required(unicode)
    checked_for_spam = Reqired(bool)

if __name__ = '__main__':
    run('localhost:8080')