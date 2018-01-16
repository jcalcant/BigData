
# coding: utf-8

# In[1]:


class BookRating(object):	
    def __init__(self, user_id, isbn, rating):
        self.user_id = user_id
        self.isbn = isbn
        try:
            self.rating = int(rating)
        except:
            self.rating = -1

#    def getUserIdAsKey(self):
#        return (self.user_id, (self.isbn, self.rating))

#    def getKeyValuePair(self):
#        return (self.isbn, (self.user_id, self.rating))

