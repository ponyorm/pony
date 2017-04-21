
from math import ceil, sqrt

class Placing:

    SPACE_HEIGHT = 30
    SPACE_WIDTH = 20

    def __init__(self, entities):
        self.entities = entities

    def apply(self):
        size = ceil(sqrt(len(self.entities)))
        top = self.SPACE_HEIGHT
        height = 0
        for row in range(size):
            left = self.SPACE_WIDTH
            for col in range(size):
                index = row * size + col
                try:
                    entity = self.entities[index]
                    h = self.get_height(entity)
                    if h > height:
                        height = h
                except IndexError:
                    return
                w = self.get_width(entity)
                entity.update({
                    'left': left, 'top': top, 'width': round(w), 'height': round(h),
                })
                left += w + self.SPACE_WIDTH
            top += height + self.SPACE_HEIGHT

    def get_height(self, entity):
        rows_count = len(entity['attrs'])
        return 68 + 22.2 * rows_count

    def get_width(self, entity):
        return 250
