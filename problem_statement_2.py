
# coding: utf-8

# In[13]:


from sqlalchemy import create_engine,Column,Integer,String


# In[3]:


engine = create_engine('sqlite:///:memory:', echo=True)


# In[4]:


from sqlalchemy.ext.declarative import declarative_base


# In[5]:


Base = declarative_base()


# In[6]:


import pandas as pd
import numpy as np


# In[11]:


column_names = ['age','workclass','fnlwgt','education','education_num','marital_status','occupation','relationship','race','sex','capital_gain','capital_loss','hours_per_week','native_country','salary']
df = pd.read_csv('https://archive.ics.uci.edu/ml/machine-learning-databases/adult/adult.data',names=column_names)


# In[12]:


df.head(5)


# In[14]:


class Adult(Base):
    __tablename__ = 'adult'
    index = Column(Integer,primary_key=True)
    age = Column(Integer)
    workclass = Column(String)
    fnlwgt = Column(Integer)
    education = Column(String)
    education_num = Column(Integer)
    marital_status = Column(String)
    occupation = Column(String)
    relationship = Column(String)
    race = Column(String)
    sex = Column(String)
    capital_gain = Column(Integer)
    capital_loss = Column(Integer)
    hours_per_week = Column(Integer)
    native_country = Column(String)
    salary = Column(String)
    
    def __repr__(self):
        return ("<Adult(age='%s', workclass='%s', marital_status='%s')>" % (self.age, self.workclass, self.marital_status))


# In[15]:


Adult.__table__


# In[16]:


Base.metadata.create_all(engine)


# In[17]:


from sqlalchemy.orm import sessionmaker


# In[18]:


Session = sessionmaker()


# In[19]:


Session.configure(bind=engine)


# In[20]:


session = Session()


# In[25]:


df.to_sql(name='adult', con=engine, if_exists = 'append', index=False)


# In[27]:


session.query(Adult).all()


# # 2. update queries

# In[29]:


from sqlalchemy import update


# In[31]:


session.query(Adult).filter(Adult.workclass == ' Private').update({'workclass': 'private'})
session.commit()


# In[32]:


session.query(Adult).all()


# ## converted all the workclass value Private -> private (all small letters)

# In[34]:


session.query(Adult).filter(Adult.marital_status == ' Married-civ-spouse').update({'marital_status': 'married_civ_spouse'})
session.commit()


# In[35]:


session.query(Adult).all()


# ## converted all the marital_status value of 'Married-civ-spouse' -> 'married_civ_spouse'

# # 3. Delete queries

# In[38]:


from sqlalchemy import delete


# In[40]:


session.query(Adult).filter(Adult.age == 24).delete()
session.commit()


# In[42]:


session.query(Adult).filter(Adult.age == 30).all()


# In[43]:


session.query(Adult).filter(Adult.age == 30).delete()
session.commit()


# In[44]:


session.query(Adult).filter(Adult.age == 30).all()


# # deleted all the rows where age is 24 and 30

# # 4. Filter queries

# In[47]:


session.query(Adult).filter(Adult.age == 34).all()


# In[48]:


session.query(Adult).filter(Adult.workclass=='private').all()


# ## filter results with age equal to 34 in first query
# 

# ## filter results with workclass equal to private in second query

# # 5. Function queries

# In[52]:


from sqlalchemy import func


# In[53]:


session.query(func.count('*')).select_from(Adult).scalar()


# In[54]:


session.query(func.count(Adult.age), Adult.age).group_by(Adult.age).all()


# ## in first query - outputted the total number of rows in Adult table

# ## in second query - outputted the frequency of the ages in the table
