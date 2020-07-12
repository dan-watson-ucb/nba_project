from django.db import models
from datetime import datetime

# Create your models here.
# Creates a new table called Tutorial and these are our columns
class Tutorial(models.Model): #inherits from Model
    tutorial_title = models.CharField(max_length= 200)
    tutorial_content = models.TextField()
    tutorial_published = models.DateTimeField("date published",
                                               default = datetime.now()) # what we're calling it

    def __str__(self):
        return self.tutorial_title