class Meta(type):
      def __iter__(self):
           return iter([])
      
class Entity(object):
    __metaclass__ = Meta
      
class Student(Entity):
      pass

