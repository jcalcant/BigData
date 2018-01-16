
# coding: utf-8

# In[1]:


class Book(object):	
    def __init__(self, isbn, title, author, year, publisher):
        self.isbn = isbn
        self.title = title
        self.author = author
        try:
            self.year = int(year)
        except:
            self.year = -1
        self.publisher = publisher

#    def getKeyValuePair(self):
#        return (self.isbn, (self.title, self.author, self.year, self.publisher))

