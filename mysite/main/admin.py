from django.contrib import admin
from .models import Tutorial
from tinymce.widgets import TinyMCE
from django.db import models

# Register your models here.

class TutorialAdmin(admin.ModelAdmin):
    # #changes order in admin
    # fields = ["tutorial_title",
    #         'tutorial_published',
    #         'tutorial_content',
    #         ]

    # creating sets to segment
    fieldsets = [
        ("Title/Date", {"fields": ['tutorial_title', 'tutorial_published']}),
        ("Content", {'fields': ['tutorial_content']})
    ]

    # add MCE only to our tutorial fields
    formfield_overrides = {
        models.TextField: {'widget': TinyMCE()}
    }

admin.site.register(Tutorial, TutorialAdmin)