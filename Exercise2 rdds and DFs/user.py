
# coding: utf-8

# In[3]:


class User(object):	
    def __init__(self, user_id, location, age):
        self.user_id = user_id
        self.location = location
        try:
            self.age = int(age)
        except:
            self.age = -1

    #def getKeyValuePair(self):
     #   return (self.user_id, (self.location, self.age))

